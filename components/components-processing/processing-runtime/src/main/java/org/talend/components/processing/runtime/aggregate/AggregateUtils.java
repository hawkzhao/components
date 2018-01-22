package org.talend.components.processing.runtime.aggregate;

import static org.talend.components.adapter.beam.kv.SchemaGeneratorUtils.TREE_ROOT_DEFAULT_VALUE;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.adapter.beam.kv.SchemaGeneratorUtils;
import org.talend.components.api.exception.error.ComponentsErrorCode;
import org.talend.components.processing.definition.aggregate.AggregateColumnFunction;
import org.talend.components.processing.definition.aggregate.AggregateFunctionProperties;
import org.talend.components.processing.definition.aggregate.AggregateGroupProperties;
import org.talend.components.processing.definition.aggregate.AggregateProperties;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.exception.error.CommonErrorCodes;

public class AggregateUtils {

    public static Schema genOutputValueSchema(Schema inputSchema, AggregateProperties props) {
        Map<String, Set<Object>> container = new HashMap<>();

        for (AggregateFunctionProperties aggFuncProps : props.filteredFunctions()) {
            fillNewFieldTreeByAggFunc(container, aggFuncProps, inputSchema);
        }

        return SchemaGeneratorUtils.convertTreeToAvroSchema(container, TREE_ROOT_DEFAULT_VALUE, inputSchema);
    }

    public static Schema genOutputFieldSchema(Schema inputSchema, AggregateFunctionProperties funcProps) {
        Map<String, Set<Object>> container = new HashMap<>();

        fillNewFieldTreeByAggFunc(container, funcProps, inputSchema);

        return SchemaGeneratorUtils.convertTreeToAvroSchema(container, TREE_ROOT_DEFAULT_VALUE, inputSchema);
    }

    private void fillNewFieldTreeByGroup(Map<String, Set<Object>> container, AggregateGroupProperties groupProps,
            Schema inputSchema) {
        fillNewFieldTree(container, groupProps.columnName.getValue(),
                SchemaGeneratorUtils.retrieveFieldFromJsonPath(inputSchema, groupProps.columnName.getValue()));
    }

    public static String genOutputColPath(AggregateFunctionProperties funcProps) {
        return StringUtils.isEmpty(funcProps.outputColumnName.getValue())
                ? genOutputColNameByFunc(funcProps.columnName.getValue(), funcProps.aggregateColumnFunction.getValue())
                : funcProps.outputColumnName.getValue();
    }

    private static String genOutputColNameByFunc(String originalName, AggregateColumnFunction func) {
        return originalName + "_" + func.toString();
    }

    private static void fillNewFieldTreeByAggFunc(Map<String, Set<Object>> container,
            AggregateFunctionProperties funcProps, Schema inputSchema) {
        String path = genOutputColPath(funcProps);
        Schema.Field newField =
                genField(SchemaGeneratorUtils.retrieveFieldFromJsonPath(inputSchema, funcProps.columnName.getValue()),
                        funcProps);

        fillNewFieldTree(container, path, newField);
    }

    public static void fillNewFieldTree(Map<String, Set<Object>> container, String path, Schema.Field field) {
        String currentParent = TREE_ROOT_DEFAULT_VALUE;
        String[] splittedPath = path.split("\\.");
        for (int i = 0; i < splittedPath.length - 1; i++) {
            if (!container.containsKey(currentParent)) {
                container.put(currentParent, new LinkedHashSet<>());
            }
            container.get(currentParent).add(currentParent + "." + splittedPath[i]);
            currentParent = currentParent + "." + splittedPath[i];
        }
        // Add the field into the tree
        if (!container.containsKey(currentParent)) {
            container.put(currentParent, new LinkedHashSet<>());
        }

        container.get(currentParent).add(field);
    }

    /**
     * Generate new field,
     * if user not set output column path, use input field name with function name
     * if user set output column path, use it, but if it is path, use the last leaf node name
     *
     * @param originalField
     * @param funcProps
     * @return
     */
    public static Schema.Field genField(Schema.Field originalField, AggregateFunctionProperties funcProps) {
        Schema newFieldSchema = genFieldType(originalField.schema(), funcProps.aggregateColumnFunction.getValue());
        String outputColPath = funcProps.outputColumnName.getValue();
        String newFieldName;
        if (StringUtils.isEmpty(outputColPath)) {
            newFieldName = genOutputColNameByFunc(originalField.name(), funcProps.aggregateColumnFunction.getValue());
        } else {
            newFieldName =
                    outputColPath.contains(".") ? StringUtils.substringAfterLast(outputColPath, ".") : outputColPath;
        }
        return new Schema.Field(newFieldName, newFieldSchema, originalField.doc(), originalField.defaultVal());
    }

    /**
     * Generate new field type,
     * assume output column type according to input column type and aggregate function
     *
     * @param fieldType
     * @param funcType
     * @return
     */
    public static Schema genFieldType(Schema fieldType, AggregateColumnFunction funcType) {
        switch (funcType) {
        case LIST: {
            return Schema.createArray(fieldType);
        }
        case COUNT: {
            return AvroUtils._long();
        }
        default:
            if (!AvroUtils.isNumerical(fieldType.getType())) {
                TalendRuntimeException.build(ComponentsErrorCode.SCHEMA_TYPE_MISMATCH).setAndThrow("aggregate",
                        "int/long/float/double", fieldType.getType().getName());
            }
            switch (funcType) {
            case SUM:
                if (AvroUtils.isSameType(fieldType, AvroUtils._int())) {
                    return AvroUtils._long();
                } else if (AvroUtils.isSameType(fieldType, AvroUtils._float())) {
                    return AvroUtils._double();
                }
                // else double and long
                return fieldType;
            case AVG:
                return AvroUtils._double();
            case MIN:
            case MAX:
                return fieldType;
            }
            TalendRuntimeException.build(CommonErrorCodes.UNEXPECTED_ARGUMENT).throwIt();
            break;
        }
        return fieldType;
    }

    public static void setField(String fieldPath, Object value, IndexedRecord record) {
        String[] path = fieldPath.split("\\.");
        if (path.length <= 0) {
            return;
        }
        Schema schema = record.getSchema();
        IndexedRecord parentRecord = record;
        for (int i = 0; i < path.length - 1; i++) {
            if (schema.getField(path[i]) == null) {
                return;
            }

            Object parentRecordObj = parentRecord.get(schema.getField(path[i]).pos());

            if (parentRecordObj instanceof GenericData.Record) {
                parentRecord = (IndexedRecord) parentRecordObj;

                // The sub-schema at this level is a union of "empty" and a
                // record, so we need to get the true sub-schema
                if (schema.getField(path[i]).schema().getType().equals(Schema.Type.RECORD)) {
                    schema = schema.getField(path[i]).schema();
                } else if (schema.getField(path[i]).schema().getType().equals(Schema.Type.UNION)) {
                    for (Schema childSchema : schema.getField(path[i]).schema().getTypes()) {
                        if (childSchema.getType().equals(Schema.Type.RECORD)) {
                            schema = childSchema;
                            break;
                        }
                    }
                }
            } else {
                // can't find parent record, can't fill value
                return;
            }
        }

        parentRecord.put(schema.getField(path[path.length - 1]).pos(), value);
    }
}

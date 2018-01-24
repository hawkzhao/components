package org.talend.components.processing.runtime.aggregate;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.talend.components.processing.definition.aggregate.AggregateColumnFunction;
import org.talend.components.processing.definition.aggregate.AggregateFunctionProperties;
import org.talend.components.processing.definition.aggregate.AggregateGroupProperties;
import org.talend.components.processing.definition.aggregate.AggregateProperties;

public class AggregateRuntimeTest {

    private final Schema basicSchema = SchemaBuilder
            .record("basic")
            .fields()
            .name("g1")
            .type()
            .stringType()
            .noDefault()
            .name("g2")
            .type()
            .stringType()
            .noDefault()
            .name("int1")
            .type()
            .unionOf()
            .intType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("long1")
            .type()
            .unionOf()
            .longType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("float1")
            .type()
            .unionOf()
            .floatType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("double1")
            .type()
            .unionOf()
            .doubleType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .endRecord();

    private final IndexedRecord basic1 = new GenericRecordBuilder(basicSchema)
            .set("g1", "teamA")
            .set("g2", "sub1")
            .set("int1", 1)
            .set("long1", 1l)
            .set("float1", 1.0f)
            .set("double1", 1.0)
            .build();

    private final IndexedRecord basic2 = new GenericRecordBuilder(basicSchema)
            .set("g1", "teamA")
            .set("g2", "sub1")
            .set("int1", 2)
            .set("long1", 2l)
            .set("float1", 2.0f)
            .set("double1", 2.0)
            .build();

    private final IndexedRecord basic3 = new GenericRecordBuilder(basicSchema)
            .set("g1", "teamA")
            .set("g2", "sub1")
            .set("int1", 3)
            .set("long1", 3l)
            .set("float1", 3.0f)
            .set("double1", 3.0)
            .build();

    private final IndexedRecord basic4 = new GenericRecordBuilder(basicSchema)
            .set("g1", "teamA")
            .set("g2", "sub2")
            .set("int1", 4)
            .set("long1", 4l)
            .set("float1", 4.0f)
            .set("double1", 4.0)
            .build();

    private final IndexedRecord basic5 = new GenericRecordBuilder(basicSchema)
            .set("g1", "teamA")
            .set("g2", "sub2")
            .set("int1", 5)
            .set("long1", 5l)
            .set("float1", 5.0f)
            .set("double1", 5.0)
            .build();

    private final IndexedRecord basic6 = new GenericRecordBuilder(basicSchema)
            .set("g1", "teamB")
            .set("g2", "sub1")
            .set("int1", 6)
            .set("long1", 6l)
            .set("float1", 6.0f)
            .set("double1", 6.0)
            .build();

    private final IndexedRecord basic7 = new GenericRecordBuilder(basicSchema)
            .set("g1", "teamC")
            .set("g2", "sub1")
            .set("int1", null)
            .set("long1", null)
            .set("float1", null)
            .set("double1", null)
            .build();

    private final List<IndexedRecord> basicList = Arrays.asList(basic1, basic2, basic3, basic4, basic5, basic6, basic7);

    private final Schema basicResultSchema = SchemaBuilder
            .record("basicResult")
            .fields()
            .name("g1")
            .type()
            .stringType()
            .noDefault()
            .name("g2")
            .type()
            .stringType()
            .noDefault()

            .name("int1_MIN")
            .type()
            .unionOf()
            .intType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("long1_MIN")
            .type()
            .unionOf()
            .longType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("float1_MIN")
            .type()
            .unionOf()
            .floatType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("double1_MIN")
            .type()
            .unionOf()
            .doubleType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()

            .name("int1_MAX")
            .type()
            .unionOf()
            .intType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("long1_MAX")
            .type()
            .unionOf()
            .longType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("float1_MAX")
            .type()
            .unionOf()
            .floatType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("double1_MAX")
            .type()
            .unionOf()
            .doubleType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()

            .name("int1_AVG")
            .type()
            .unionOf()
            .doubleType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("long1_AVG")
            .type()
            .unionOf()
            .doubleType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("float1_AVG")
            .type()
            .unionOf()
            .doubleType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("double1_AVG")
            .type()
            .unionOf()
            .doubleType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()

            .name("int1_SUM")
            .type()
            .unionOf()
            .longType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("long1_SUM")
            .type()
            .unionOf()
            .longType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("float1_SUM")
            .type()
            .unionOf()
            .doubleType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()
            .name("double1_SUM")
            .type()
            .unionOf()
            .doubleType()
            .and()
            .nullType()
            .endUnion()
            .noDefault()

            .name("g2_list_value")
            .type()
            .array()
            .items()
            .stringType()
            .noDefault()

            .name("g1_count_number")
            .type()
            .longType()
            .noDefault()

            .endRecord();

    private final IndexedRecord basicResult1 = new GenericRecordBuilder(basicResultSchema) //
            .set("g1", "teamA")
            .set("g2", "sub1")
            .set("int1_MIN", 1)
            .set("long1_MIN", 1l)
            .set("float1_MIN", 1.0f)
            .set("double1_MIN", 1.0)
            .set("int1_MAX", 3)
            .set("long1_MAX", 3l)
            .set("float1_MAX", 3.0f)
            .set("double1_MAX", 3.0)
            .set("int1_AVG", 2.0)
            .set("long1_AVG", 2.0)
            .set("float1_AVG", 2.0)
            .set("double1_AVG", 2.0)
            .set("int1_SUM", 6l)
            .set("long1_SUM", 6l)
            .set("float1_SUM", 6.0)
            .set("double1_SUM", 6.0)
            .set("g2_list_value", Arrays.asList("sub1", "sub1", "sub1"))
            .set("g1_count_number", 3l)
            .build();

    private final IndexedRecord basicResult2 = new GenericRecordBuilder(basicResultSchema) //
            .set("g1", "teamA")
            .set("g2", "sub2")
            .set("int1_MIN", 4)
            .set("long1_MIN", 4l)
            .set("float1_MIN", 4.0f)
            .set("double1_MIN", 4.0)
            .set("int1_MAX", 5)
            .set("long1_MAX", 5l)
            .set("float1_MAX", 5.0f)
            .set("double1_MAX", 5.0)
            .set("int1_AVG", 4.5)
            .set("long1_AVG", 4.5)
            .set("float1_AVG", 4.5)
            .set("double1_AVG", 4.5)
            .set("int1_SUM", 9l)
            .set("long1_SUM", 9l)
            .set("float1_SUM", 9.0)
            .set("double1_SUM", 9.0)
            .set("g2_list_value", Arrays.asList("sub2", "sub2"))
            .set("g1_count_number", 2l)
            .build();

    private final IndexedRecord basicResult3 = new GenericRecordBuilder(basicResultSchema) //
            .set("g1", "teamB")
            .set("g2", "sub1")
            .set("int1_MIN", 6)
            .set("long1_MIN", 6l)
            .set("float1_MIN", 6.0f)
            .set("double1_MIN", 6.0)
            .set("int1_MAX", 6)
            .set("long1_MAX", 6l)
            .set("float1_MAX", 6.0f)
            .set("double1_MAX", 6.0)
            .set("int1_AVG", 6.0)
            .set("long1_AVG", 6.0)
            .set("float1_AVG", 6.0)
            .set("double1_AVG", 6.0)
            .set("int1_SUM", 6l)
            .set("long1_SUM", 6l)
            .set("float1_SUM", 6.0)
            .set("double1_SUM", 6.0)
            .set("g2_list_value", Arrays.asList("sub1"))
            .set("g1_count_number", 1l)
            .build();

    private final IndexedRecord basicResult4 = new GenericRecordBuilder(basicResultSchema) //
            .set("g1", "teamC")
            .set("g2", "sub1")
            .set("int1_MIN", null)
            .set("long1_MIN", null)
            .set("float1_MIN", null)
            .set("double1_MIN", null)
            .set("int1_MAX", null)
            .set("long1_MAX", null)
            .set("float1_MAX", null)
            .set("double1_MAX", null)
            .set("int1_AVG", null)
            .set("long1_AVG", null)
            .set("float1_AVG", null)
            .set("double1_AVG", null)
            .set("int1_SUM", null)
            .set("long1_SUM", null)
            .set("float1_SUM", null)
            .set("double1_SUM", null)
            .set("g2_list_value", Arrays.asList("sub1"))
            .set("g1_count_number", 1l)
            .build();

    private final List<IndexedRecord> minIntResList =
            Arrays.asList(basicResult1, basicResult2, basicResult3, basicResult4);

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    private void addIntoGroup(AggregateProperties props, List<String> groupPaths) {
        for (String groupPath : groupPaths) {
            AggregateGroupProperties groupProps = new AggregateGroupProperties("group");
            groupProps.init();
            groupProps.columnName.setValue(groupPath);
            props.groupBy.addRow(groupProps);
        }
    }

    private void addIntoFunction(AggregateProperties props, AggregateColumnFunction func, List<String> columnPaths) {
        for (String columnPath : columnPaths) {
            AggregateFunctionProperties funcProps = new AggregateFunctionProperties("function");
            funcProps.init();
            funcProps.columnName.setValue(columnPath);
            funcProps.aggregateColumnFunction.setValue(func);
            props.functions.addRow(funcProps);
        }
    }

    private void addIntoFunction(AggregateProperties props, AggregateColumnFunction func, String columnPath,
            String outputColumnPath) {
        AggregateFunctionProperties funcProps = new AggregateFunctionProperties("function");
        funcProps.init();
        funcProps.columnName.setValue(columnPath);
        funcProps.aggregateColumnFunction.setValue(func);
        funcProps.outputColumnName.setValue(outputColumnPath);
        props.functions.addRow(funcProps);
    }

    @Test
    public void basicTest() {
        AggregateRuntime aggregateRuntime = new AggregateRuntime();
        AggregateProperties props = new AggregateProperties("aggregate");
        props.init();

        addIntoGroup(props, Arrays.asList("g1", "g2"));

        addIntoFunction(props, AggregateColumnFunction.MIN, Arrays.asList("int1", "long1", "float1", "double1"));

        addIntoFunction(props, AggregateColumnFunction.MAX, Arrays.asList("int1", "long1", "float1", "double1"));

        addIntoFunction(props, AggregateColumnFunction.AVG, Arrays.asList("int1", "long1", "float1", "double1"));

        addIntoFunction(props, AggregateColumnFunction.SUM, Arrays.asList("int1", "long1", "float1", "double1"));

        addIntoFunction(props, AggregateColumnFunction.LIST, "g2", "g2_list_value");

        addIntoFunction(props, AggregateColumnFunction.COUNT, "g1", "g1_count_number");

        aggregateRuntime.initialize(null, props);

        PCollection<IndexedRecord> result = pipeline.apply(Create.of(basicList)).apply(aggregateRuntime);
        PAssert.that(result).containsInAnyOrder(minIntResList);

        pipeline.run();
    }
}

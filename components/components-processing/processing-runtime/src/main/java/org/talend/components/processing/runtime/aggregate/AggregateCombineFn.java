package org.talend.components.processing.runtime.aggregate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.Combine;
import org.talend.components.adapter.beam.kv.KeyValueUtils;
import org.talend.components.adapter.beam.kv.SchemaGeneratorUtils;
import org.talend.components.processing.definition.aggregate.AggregateColumnFunction;
import org.talend.components.processing.definition.aggregate.AggregateFunctionProperties;
import org.talend.components.processing.definition.aggregate.AggregateProperties;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.exception.error.CommonErrorCodes;

public class AggregateCombineFn
        extends Combine.CombineFn<IndexedRecord, AggregateCombineFn.AggregateAccumulator, IndexedRecord> {

    private AggregateProperties properties;

    public AggregateCombineFn(AggregateProperties properties) {
        this.properties = properties;
    }

    @Override
    public AggregateAccumulator createAccumulator() {
        AggregateAccumulator accumulator = new AggregateAccumulator();
        List<AccumulatorElement> accs = new ArrayList();
        for (AggregateFunctionProperties funcProps : properties.filteredFunctions()) {
            accs.add(new AccumulatorElement(funcProps));
        }
        accumulator.accumulatorElements = accs;
        return accumulator;
    }

    @Override
    public AggregateAccumulator addInput(AggregateAccumulator accumulator, IndexedRecord input) {
        if (accumulator.outputSchemaStr == null) {
            accumulator.outputSchemaStr = AggregateUtils.genOutputValueSchema(input.getSchema(), properties).toString();
        }
        for (AccumulatorElement accumulatorElement : accumulator.accumulatorElements) {
            accumulatorElement.addInput(input);
        }
        return accumulator;
    }

    @Override
    public AggregateAccumulator mergeAccumulators(Iterable<AggregateAccumulator> accumulators) {
        AggregateAccumulator deltaAcc = createAccumulator();
        for (int idx = 0; idx < properties.filteredFunctions().size(); idx++) {
            List accs = new ArrayList();
            for (AggregateAccumulator accumulator : accumulators) {
                if (deltaAcc.outputSchemaStr == null) {
                    deltaAcc.outputSchemaStr = accumulator.outputSchemaStr;
                }
                accs.add(accumulator.accumulatorElements.get(idx));
            }
            deltaAcc.accumulatorElements.get(idx).mergeAccumulators(accs);
        }
        return deltaAcc;
    }

    @Override
    public IndexedRecord extractOutput(AggregateAccumulator accumulator) {
        Schema.Parser parser = new Schema.Parser();
        Schema outputSchema = parser.parse(accumulator.outputSchemaStr);
        IndexedRecord record = new GenericData.Record(outputSchema);
        for (AccumulatorElement accumulatorElement : accumulator.accumulatorElements) {
            IndexedRecord outputFieldRecord = accumulatorElement.extractOutput();
            if (outputFieldRecord != null) {
                record = KeyValueUtils.mergeIndexedRecord(outputFieldRecord, record, outputSchema);
            }
        }
        return record;
    }

    public static class AggregateAccumulator {

        // for merge the final output record, init by first coming record
        private String outputSchemaStr;

        // based on the defined func group
        private List<AccumulatorElement> accumulatorElements = new ArrayList();
    }

    public static class AccumulatorElement {

        AggregateFunctionProperties funcProps;

        String inputColPath;

        String outputColPath;

        AggregateColumnFunction func;

        // init by first coming record
        AccumulatorFn accumulatorFn;

        String outputFieldSchemaStr;

        public AccumulatorElement(AggregateFunctionProperties funcProps) {
            this.funcProps = funcProps;
            this.inputColPath = funcProps.columnName.getValue();
            this.outputColPath = AggregateUtils.genOutputColPath(funcProps);
            this.func = funcProps.aggregateColumnFunction.getValue();
        }

        public void addInput(IndexedRecord inputRecord) {
            if (this.outputFieldSchemaStr == null) {
                this.outputFieldSchemaStr =
                        AggregateUtils.genOutputFieldSchema(inputRecord.getSchema(), funcProps).toString();
            }
            Object field = KeyValueUtils.getField(inputColPath, inputRecord);
            if (accumulatorFn == null) {
                Schema inputColSchema =
                        SchemaGeneratorUtils.retrieveFieldFromJsonPath(inputRecord.getSchema(), inputColPath).schema();
                accumulatorFn = getProperCombineFn(inputColSchema, func);
                accumulatorFn.createAccumulator();
            }
            accumulatorFn.addInput(field);
        }

        public void mergeAccumulators(Iterable<AccumulatorElement> accumulators) {
            Iterator<AccumulatorElement> iterator = accumulators.iterator();
            if (iterator.hasNext()) {
                List accs = new ArrayList();
                while (iterator.hasNext()) {
                    AccumulatorElement next = iterator.next();
                    if (this.outputFieldSchemaStr == null) {
                        this.outputFieldSchemaStr = next.outputFieldSchemaStr;
                    }
                    if (this.accumulatorFn == null) {
                        this.accumulatorFn = next.accumulatorFn;
                        continue;
                    }
                    accs.add(next.accumulatorFn.getAccumulators());
                }
                this.accumulatorFn.mergeAccumulators(accs);
            }
        }

        public IndexedRecord extractOutput() {
            Schema.Parser parser = new Schema.Parser();
            Schema outputFieldSchema = parser.parse(outputFieldSchemaStr);
            GenericData.Record outputFieldRecord = new GenericData.Record(outputFieldSchema);
            AggregateUtils.setField(outputColPath, this.accumulatorFn.extractOutput(), outputFieldRecord);
            return outputFieldRecord;
        }

        private AccumulatorFn getProperCombineFn(Schema inputColSchema, AggregateColumnFunction func) {
            inputColSchema = AvroUtils.unwrapIfNullable(inputColSchema);
            switch (func) {

            case LIST:
                return new ListAccumulatorFn();
            case COUNT:
                return new CountAccumulatorFn();
            case SUM:
                if (AvroUtils.isSameType(inputColSchema, AvroUtils._int())
                        || AvroUtils.isSameType(inputColSchema, AvroUtils._long())) {
                    return new SumLongAccumulatorFn();
                } else if (AvroUtils.isSameType(inputColSchema, AvroUtils._float())
                        || AvroUtils.isSameType(inputColSchema, AvroUtils._double())) {
                    return new SumDoubleAccumulatorFn();
                }
            case AVG:
                return new AvgAccumulatorFn();
            case MIN:
                if (AvroUtils.isSameType(inputColSchema, AvroUtils._int())) {
                    return new MinIntegerAccumulatorFn();
                } else if (AvroUtils.isSameType(inputColSchema, AvroUtils._long())) {
                    return new MinLongAccumulatorFn();
                } else if (AvroUtils.isSameType(inputColSchema, AvroUtils._float())) {
                    return new MinFloatAccumulatorFn();
                } else if (AvroUtils.isSameType(inputColSchema, AvroUtils._double())) {
                    return new MinDoubleAccumulatorFn();
                }
            case MAX:
                if (AvroUtils.isSameType(inputColSchema, AvroUtils._int())) {
                    return new MaxIntegerAccumulatorFn();
                } else if (AvroUtils.isSameType(inputColSchema, AvroUtils._long())) {
                    return new MaxLongAccumulatorFn();
                } else if (AvroUtils.isSameType(inputColSchema, AvroUtils._float())) {
                    return new MaxFloatAccumulatorFn();
                } else if (AvroUtils.isSameType(inputColSchema, AvroUtils._double())) {
                    return new MaxDoubleAccumulatorFn();
                }
            }
            TalendRuntimeException.build(CommonErrorCodes.UNEXPECTED_ARGUMENT).throwIt();
            return null;
        }
    }

    public interface AccumulatorFn<AccumT, OutputT> {

        public void createAccumulator();

        public void addInput(Object inputValue);

        public void mergeAccumulators(Iterable<AccumT> accsList);

        public AccumT getAccumulators();

        public OutputT extractOutput();
    }

    public static class AvgAcc {

        Double sum;

        Long count = 0l;
    }

    public static class AvgAccumulatorFn implements AccumulatorFn<AvgAcc, Double> {

        AvgAcc accs;

        @Override
        public void createAccumulator() {
            accs = new AvgAcc();
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                Double value = Double.valueOf(String.valueOf(inputValue));
                accs.sum = accs.sum == null ? value : accs.sum + value;
            }
            accs.count += 1;
        }

        @Override
        public void mergeAccumulators(Iterable<AvgAcc> accsList) {
            for (AvgAcc acc : accsList) {
                if (acc.sum != null) {
                    accs.sum = accs.sum == null ? acc.sum : accs.sum + acc.sum;
                }
                this.accs.count += acc.count;
            }
        }

        @Override
        public AvgAcc getAccumulators() {
            return accs;
        }

        @Override
        public Double extractOutput() {
            return accs.sum == null ? null : (accs.count == 0l ? 0.0 : accs.sum / accs.count);
        }
    }

    public static abstract class AccumulatorFnAbstract<T> implements AccumulatorFn<T, T> {

        T accs;

        @Override
        public void createAccumulator() {
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                T value = convertFromObject(inputValue);
                accs = accs == null ? value : apply(value, accs);
            }
        }

        @Override
        public void mergeAccumulators(Iterable<T> accsList) {
            for (T acc : accsList) {
                if (acc == null) {
                    continue;
                }
                accs = accs == null ? acc : apply(acc, accs);
            }
        }

        @Override
        public T getAccumulators() {
            return accs;
        }

        @Override
        public T extractOutput() {
            return accs;
        }

        public abstract T convertFromObject(Object inputValue);

        public abstract T apply(T left, T right);

    }

    public static class SumDoubleAccumulatorFn extends AccumulatorFnAbstract<Double> {

        @Override
        public Double convertFromObject(Object inputValue) {
            return Double.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Double apply(Double left, Double right) {
            return left + right;
        }
    }

    public static class SumLongAccumulatorFn extends AccumulatorFnAbstract<Long> {

        @Override
        public Long convertFromObject(Object inputValue) {
            return Long.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Long apply(Long left, Long right) {
            return left + right;
        }
    }

    public static class CountAccumulatorFn implements AccumulatorFn<Long, Long> {

        Long accs;

        @Override
        public void createAccumulator() {
            accs = 0l;
        }

        @Override
        public void addInput(Object inputValue) {
            accs += 1;
        }

        @Override
        public void mergeAccumulators(Iterable<Long> accsList) {
            Iterator<Long> iterator = accsList.iterator();
            if (!iterator.hasNext()) {
                createAccumulator();
            }
            while (iterator.hasNext()) {
                accs += iterator.next();
            }
        }

        @Override
        public Long getAccumulators() {
            return accs;
        }

        @Override
        public Long extractOutput() {
            return accs;
        }
    }

    public static class ListAccumulatorFn implements AccumulatorFn<List, List> {

        List accs;

        @Override
        public void createAccumulator() {
            accs = new ArrayList();
        }

        @Override
        public void addInput(Object inputValue) {
            accs.add(inputValue);
        }

        @Override
        public void mergeAccumulators(Iterable<List> accsList) {
            for (List acc : accsList) {
                accs.addAll(acc);
            }
        }

        @Override
        public List getAccumulators() {
            return accs;
        }

        @Override
        public List extractOutput() {
            return accs;
        }
    }

    public static class MinIntegerAccumulatorFn extends AccumulatorFnAbstract<Integer> {

        @Override
        public Integer convertFromObject(Object inputValue) {
            return Integer.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Integer apply(Integer left, Integer right) {
            return left < right ? left : right;
        }
    }

    public static class MinLongAccumulatorFn extends AccumulatorFnAbstract<Long> {

        @Override
        public Long convertFromObject(Object inputValue) {
            return Long.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Long apply(Long left, Long right) {
            return left < right ? left : right;
        }
    }

    public static class MinFloatAccumulatorFn extends AccumulatorFnAbstract<Float> {

        @Override
        public Float convertFromObject(Object inputValue) {
            return Float.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Float apply(Float left, Float right) {
            return left < right ? left : right;
        }
    }

    public static class MinDoubleAccumulatorFn extends AccumulatorFnAbstract<Double> {

        @Override
        public Double convertFromObject(Object inputValue) {
            return Double.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Double apply(Double left, Double right) {
            return left < right ? left : right;
        }
    }

    public static class MaxIntegerAccumulatorFn extends AccumulatorFnAbstract<Integer> {

        @Override
        public Integer convertFromObject(Object inputValue) {
            return Integer.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Integer apply(Integer left, Integer right) {
            return left > right ? left : right;
        }
    }

    public static class MaxLongAccumulatorFn extends AccumulatorFnAbstract<Long> {

        @Override
        public Long convertFromObject(Object inputValue) {
            return Long.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Long apply(Long left, Long right) {
            return left > right ? left : right;
        }
    }

    public static class MaxFloatAccumulatorFn extends AccumulatorFnAbstract<Float> {

        @Override
        public Float convertFromObject(Object inputValue) {
            return Float.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Float apply(Float left, Float right) {
            return left > right ? left : right;
        }
    }

    public static class MaxDoubleAccumulatorFn extends AccumulatorFnAbstract<Double> {

        @Override
        public Double convertFromObject(Object inputValue) {
            return Double.valueOf(String.valueOf(inputValue));
        }

        @Override
        public Double apply(Double left, Double right) {
            return left > right ? left : right;
        }
    }

}

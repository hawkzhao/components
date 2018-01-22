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
        IndexedRecord record = null;
        for (AccumulatorElement accumulatorElement : accumulator.accumulatorElements) {
            IndexedRecord outputFieldRecord = accumulatorElement.extractOutput();
            if (outputFieldRecord != null) {
                record = KeyValueUtils.mergeIndexedRecord(outputFieldRecord, new GenericData.Record(outputSchema),
                        outputSchema);
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

        public void mergeAccumulators(Iterable<AccumT> accs);

        public AccumT getAccumulators();

        public OutputT extractOutput();
    }

    public static class AvgAcc {

        Double sum = 0.0;

        Long count = 0l;
    }

    public static class AvgAccumulatorFn implements AccumulatorFn<AvgAcc, Double> {

        AvgAcc accs;

        @Override
        public void createAccumulator() {
            this.accs = new AvgAcc();
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs.sum += Double.valueOf(String.valueOf(inputValue));
                this.accs.count += 1;
            }
        }

        @Override
        public void mergeAccumulators(Iterable<AvgAcc> accs) {
            for (AvgAcc acc : accs) {
                this.accs.sum += acc.sum;
                this.accs.count += acc.count;
            }
        }

        @Override
        public AvgAcc getAccumulators() {
            return accs;
        }

        @Override
        public Double extractOutput() {
            return accs.count == 0l ? 0.0 : accs.sum / accs.count;
        }
    }

    public static class SumDoubleAccumulatorFn implements AccumulatorFn<double[], Double> {

        double[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new double[] { 0.0 };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] += Double.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<double[]> accs) {
            for (double[] acc : accs) {
                this.accs[0] += acc[0];
            }
        }

        @Override
        public double[] getAccumulators() {
            return accs;
        }

        @Override
        public Double extractOutput() {
            return accs[0];
        }
    }

    public static class SumLongAccumulatorFn implements AccumulatorFn<long[], Long> {

        long[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new long[] { 0l };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] += Long.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<long[]> accs) {
            for (long[] acc : accs) {
                this.accs[0] += acc[0];
            }
        }

        @Override
        public long[] getAccumulators() {
            return accs;
        }

        @Override
        public Long extractOutput() {
            return accs[0];
        }
    }

    public static class CountAccumulatorFn implements AccumulatorFn<long[], Long> {

        long[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new long[] { 0 };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) { // TODO how to check node is null, or accumulative no matter what it is
                this.accs[0] += 1;
            }
        }

        @Override
        public void mergeAccumulators(Iterable<long[]> accs) {
            Iterator<long[]> iterator = accs.iterator();
            if (!iterator.hasNext()) {
                createAccumulator();
            }
            while (iterator.hasNext()) {
                this.accs[0] += iterator.next()[0];
            }
        }

        @Override
        public long[] getAccumulators() {
            return accs;
        }

        @Override
        public Long extractOutput() {
            return accs[0];
        }
    }

    public static class ListAccumulatorFn implements AccumulatorFn<List, List> {

        List accs;

        @Override
        public void createAccumulator() {
            this.accs = new ArrayList();
        }

        @Override
        public void addInput(Object inputValue) {
            this.accs.add(inputValue);
        }

        @Override
        public void mergeAccumulators(Iterable<List> accs) {
            for (List acc : accs) {
                this.accs.addAll(acc);
            }
        }

        @Override
        public List getAccumulators() {
            return this.accs;
        }

        @Override
        public List extractOutput() {
            return getAccumulators();
        }
    }

    public static class MinIntegerAccumulatorFn implements AccumulatorFn<int[], Integer> {

        int[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new int[] { Integer.MAX_VALUE };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] = this.accs[0] < Integer.valueOf(String.valueOf(inputValue)) ? this.accs[0]
                        : Integer.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<int[]> accs) {
            for (int[] acc : accs) {
                this.accs[0] = this.accs[0] < acc[0] ? this.accs[0] : acc[0];
            }
        }

        @Override
        public int[] getAccumulators() {
            return this.accs;
        }

        @Override
        public Integer extractOutput() {
            return this.accs[0];
        }
    }

    public static class MinLongAccumulatorFn implements AccumulatorFn<long[], Long> {

        long[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new long[] { Long.MAX_VALUE };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] = this.accs[0] < Long.valueOf(String.valueOf(inputValue)) ? this.accs[0]
                        : Long.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<long[]> accs) {
            for (long[] acc : accs) {
                this.accs[0] = this.accs[0] < acc[0] ? this.accs[0] : acc[0];
            }
        }

        @Override
        public long[] getAccumulators() {
            return this.accs;
        }

        @Override
        public Long extractOutput() {
            return this.accs[0];
        }
    }

    public static class MinFloatAccumulatorFn implements AccumulatorFn<float[], Float> {

        float[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new float[] { Float.MAX_VALUE };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] = this.accs[0] < Float.valueOf(String.valueOf(inputValue)) ? this.accs[0]
                        : Float.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<float[]> accs) {
            for (float[] acc : accs) {
                this.accs[0] = this.accs[0] < acc[0] ? this.accs[0] : acc[0];
            }
        }

        @Override
        public float[] getAccumulators() {
            return this.accs;
        }

        @Override
        public Float extractOutput() {
            return this.accs[0];
        }
    }

    public static class MinDoubleAccumulatorFn implements AccumulatorFn<double[], Double> {

        double[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new double[] { Double.MAX_VALUE };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] = this.accs[0] < Double.valueOf(String.valueOf(inputValue)) ? this.accs[0]
                        : Double.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<double[]> accs) {
            for (double[] acc : accs) {
                this.accs[0] = this.accs[0] < acc[0] ? this.accs[0] : acc[0];
            }
        }

        @Override
        public double[] getAccumulators() {
            return this.accs;
        }

        @Override
        public Double extractOutput() {
            return this.accs[0];
        }
    }

    public static class MaxIntegerAccumulatorFn implements AccumulatorFn<int[], Integer> {

        int[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new int[] { Integer.MIN_VALUE };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] = this.accs[0] > Integer.valueOf(String.valueOf(inputValue)) ? this.accs[0]
                        : Integer.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<int[]> accs) {
            for (int[] acc : accs) {
                this.accs[0] = this.accs[0] > acc[0] ? this.accs[0] : acc[0];
            }
        }

        @Override
        public int[] getAccumulators() {
            return this.accs;
        }

        @Override
        public Integer extractOutput() {
            return this.accs[0];
        }
    }

    public static class MaxLongAccumulatorFn implements AccumulatorFn<long[], Long> {

        long[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new long[] { Long.MIN_VALUE };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] = this.accs[0] > Long.valueOf(String.valueOf(inputValue)) ? this.accs[0]
                        : Long.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<long[]> accs) {
            for (long[] acc : accs) {
                this.accs[0] = this.accs[0] > acc[0] ? this.accs[0] : acc[0];
            }
        }

        @Override
        public long[] getAccumulators() {
            return this.accs;
        }

        @Override
        public Long extractOutput() {
            return this.accs[0];
        }
    }

    public static class MaxFloatAccumulatorFn implements AccumulatorFn<float[], Float> {

        float[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new float[] { Float.MIN_VALUE };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] = this.accs[0] > Float.valueOf(String.valueOf(inputValue)) ? this.accs[0]
                        : Float.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<float[]> accs) {
            for (float[] acc : accs) {
                this.accs[0] = this.accs[0] > acc[0] ? this.accs[0] : acc[0];
            }
        }

        @Override
        public float[] getAccumulators() {
            return this.accs;
        }

        @Override
        public Float extractOutput() {
            return this.accs[0];
        }
    }

    public static class MaxDoubleAccumulatorFn implements AccumulatorFn<double[], Double> {

        double[] accs;

        @Override
        public void createAccumulator() {
            this.accs = new double[] { Double.MIN_VALUE };
        }

        @Override
        public void addInput(Object inputValue) {
            if (inputValue != null) {
                this.accs[0] = this.accs[0] > Double.valueOf(String.valueOf(inputValue)) ? this.accs[0]
                        : Double.valueOf(String.valueOf(inputValue));
            }
        }

        @Override
        public void mergeAccumulators(Iterable<double[]> accs) {
            for (double[] acc : accs) {
                this.accs[0] = this.accs[0] > acc[0] ? this.accs[0] : acc[0];
            }
        }

        @Override
        public double[] getAccumulators() {
            return this.accs;
        }

        @Override
        public Double extractOutput() {
            return this.accs[0];
        }
    }

}

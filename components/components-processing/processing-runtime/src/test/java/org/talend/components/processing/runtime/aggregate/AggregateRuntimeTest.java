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

    private final Schema minIntSchema = SchemaBuilder
            .record("minInt") //
            .fields() //
            .name("g1")
            .type()
            .stringType()
            .noDefault() //
            .name("v1")
            .type()
            .intType()
            .noDefault() //
            .endRecord();

    private final IndexedRecord minInt1 = new GenericRecordBuilder(minIntSchema) //
            .set("g1", "teamA")
            .set("v1", 1)
            .build();

    private final IndexedRecord minInt2 = new GenericRecordBuilder(minIntSchema) //
            .set("g1", "teamA")
            .set("v1", 10)
            .build();

    private final IndexedRecord minInt3 = new GenericRecordBuilder(minIntSchema) //
            .set("g1", "teamA")
            .set("v1", 5)
            .build();

    private final IndexedRecord minInt4 = new GenericRecordBuilder(minIntSchema) //
            .set("g1", "teamB")
            .set("v1", 2)
            .build();

    private final List<IndexedRecord> minIntList = Arrays.asList(minInt1, minInt2, minInt3, minInt4);

    private final Schema minIntResSchema = SchemaBuilder
            .record("minInt") //
            .fields() //
            .name("g1")
            .type()
            .stringType()
            .noDefault() //
            .name("m1")
            .type()
            .intType()
            .noDefault() //
            .endRecord();

    private final IndexedRecord minIntRes1 = new GenericRecordBuilder(minIntResSchema) //
            .set("g1", "teamA")
            .set("m1", 1)
            .build();

    private final IndexedRecord minIntRes2 = new GenericRecordBuilder(minIntResSchema) //
            .set("g1", "teamB")
            .set("m1", 2)
            .build();

    private final List<IndexedRecord> minIntResList = Arrays.asList(minIntRes1, minIntRes2);

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void basicTest() {
        AggregateRuntime aggregateRuntime = new AggregateRuntime();
        AggregateProperties props = new AggregateProperties("aggregate");
        props.init();
        AggregateGroupProperties groupProps = new AggregateGroupProperties("group");
        groupProps.init();
        groupProps.columnName.setValue("g1");
        props.groupBy.addRow(groupProps);
        AggregateFunctionProperties funcProps = new AggregateFunctionProperties("function");
        funcProps.init();
        funcProps.columnName.setValue("v1");
        funcProps.aggregateColumnFunction.setValue(AggregateColumnFunction.MIN);
        funcProps.outputColumnName.setValue("m1");
        props.functions.addRow(funcProps);
        aggregateRuntime.initialize(null, props);

        PCollection<IndexedRecord> result = pipeline.apply(Create.of(minIntList)).apply(aggregateRuntime);
        PAssert.that(result).containsInAnyOrder(minIntResList);

        pipeline.run();
    }
}

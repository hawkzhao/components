package org.talend.components.processing.runtime.aggregate;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.talend.components.processing.definition.aggregate.AggregateColumnFunction;
import org.talend.components.processing.definition.aggregate.AggregateFunctionProperties;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.exception.TalendRuntimeException;

public class AggregateUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void genField() {
        {
            Schema.Field f1 = new Schema.Field("f1", AvroUtils._string(), "", "");
            AggregateFunctionProperties funcProps = new AggregateFunctionProperties("");
            funcProps.aggregateColumnFunction.setValue(AggregateColumnFunction.COUNT);
            Schema.Field newField = AggregateUtils.genField(f1, funcProps);
            Assert.assertEquals("f1_COUNT", newField.name());
            Assert.assertEquals(AvroUtils._long(), AvroUtils.unwrapIfNullable(newField.schema()));
        }

        {
            Schema.Field f2 = new Schema.Field("f2", AvroUtils._string(), "", "");
            AggregateFunctionProperties funcProps = new AggregateFunctionProperties("");
            funcProps.aggregateColumnFunction.setValue(AggregateColumnFunction.COUNT);
            funcProps.outputColumnName.setValue("new_f2");
            Schema.Field newField = AggregateUtils.genField(f2, funcProps);
            Assert.assertEquals("new_f2", newField.name());
            Assert.assertEquals(AvroUtils._long(), AvroUtils.unwrapIfNullable(newField.schema()));
        }

        {
            // test custom output path with `new.f3`, it will return `f3` only, and `new` will be the parent element
            Schema.Field f3 = new Schema.Field("f3", AvroUtils._string(), "", "");
            AggregateFunctionProperties funcProps = new AggregateFunctionProperties("");
            funcProps.aggregateColumnFunction.setValue(AggregateColumnFunction.COUNT);
            funcProps.outputColumnName.setValue("new.f3");
            Schema.Field newField = AggregateUtils.genField(f3, funcProps);
            Assert.assertEquals("f3", newField.name());
            Assert.assertEquals(AvroUtils._long(), AvroUtils.unwrapIfNullable(newField.schema()));
        }
    }

    @Test
    public void genFieldType() {
        Schema schema = AggregateUtils.genFieldType(AvroUtils._int(), AggregateColumnFunction.LIST);
        Assert.assertEquals(Schema.Type.ARRAY, schema.getType());
        Assert.assertTrue(AvroUtils.isSameType(schema.getElementType(), AvroUtils._int()));

        schema = AggregateUtils.genFieldType(AvroUtils._string(), AggregateColumnFunction.COUNT);
        Assert.assertTrue(AvroUtils.isSameType(schema, AvroUtils._long()));

        thrown.expect(TalendRuntimeException.class);
        AggregateUtils.genFieldType(AvroUtils._string(), AggregateColumnFunction.SUM);
        schema = AggregateUtils.genFieldType(AvroUtils._int(), AggregateColumnFunction.SUM);
        Assert.assertEquals(AvroUtils._long(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._long(), AggregateColumnFunction.SUM);
        Assert.assertEquals(AvroUtils._long(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._float(), AggregateColumnFunction.SUM);
        Assert.assertEquals(AvroUtils._double(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._double(), AggregateColumnFunction.SUM);
        Assert.assertEquals(AvroUtils._double(), schema);

        schema = AggregateUtils.genFieldType(AvroUtils._int(), AggregateColumnFunction.AVG);
        Assert.assertEquals(AvroUtils._double(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._long(), AggregateColumnFunction.AVG);
        Assert.assertEquals(AvroUtils._double(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._float(), AggregateColumnFunction.AVG);
        Assert.assertEquals(AvroUtils._double(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._double(), AggregateColumnFunction.AVG);
        Assert.assertEquals(AvroUtils._double(), schema);

        schema = AggregateUtils.genFieldType(AvroUtils._int(), AggregateColumnFunction.MIN);
        Assert.assertEquals(AvroUtils._int(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._long(), AggregateColumnFunction.MIN);
        Assert.assertEquals(AvroUtils._long(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._float(), AggregateColumnFunction.MIN);
        Assert.assertEquals(AvroUtils._float(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._double(), AggregateColumnFunction.MIN);
        Assert.assertEquals(AvroUtils._double(), schema);

        schema = AggregateUtils.genFieldType(AvroUtils._int(), AggregateColumnFunction.MAX);
        Assert.assertEquals(AvroUtils._int(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._long(), AggregateColumnFunction.MAX);
        Assert.assertEquals(AvroUtils._long(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._float(), AggregateColumnFunction.MAX);
        Assert.assertEquals(AvroUtils._float(), schema);
        schema = AggregateUtils.genFieldType(AvroUtils._double(), AggregateColumnFunction.MAX);
        Assert.assertEquals(AvroUtils._double(), schema);
    }
}

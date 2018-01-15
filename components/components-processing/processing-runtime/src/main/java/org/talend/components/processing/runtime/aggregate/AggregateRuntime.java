package org.talend.components.processing.runtime.aggregate;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.talend.components.api.component.runtime.RuntimableRuntime;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.processing.definition.aggregate.AggregateProperties;
import org.talend.daikon.properties.ValidationResult;

public class AggregateRuntime extends PTransform<PCollection<IndexedRecord>, PCollection<IndexedRecord>>
        implements RuntimableRuntime<AggregateProperties> {

    private AggregateProperties properties;

    @Override
    public PCollection<IndexedRecord> expand(PCollection<IndexedRecord> indexedRecordPCollection) {
        return null;
    }

    @Override
    public ValidationResult initialize(RuntimeContainer container, AggregateProperties properties) {
        this.properties = properties;
        return ValidationResult.OK;
    }
}

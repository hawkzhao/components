package org.talend.components.processing.runtime.aggregate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.talend.components.adapter.beam.coders.LazyAvroCoder;
import org.talend.components.adapter.beam.kv.ExtractKVFn;
import org.talend.components.adapter.beam.kv.MergeKVFn;
import org.talend.components.api.component.runtime.RuntimableRuntime;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.processing.definition.aggregate.AggregateFunctionProperties;
import org.talend.components.processing.definition.aggregate.AggregateGroupProperties;
import org.talend.components.processing.definition.aggregate.AggregateProperties;
import org.talend.daikon.properties.ValidationResult;

public class AggregateRuntime extends PTransform<PCollection<IndexedRecord>, PCollection<IndexedRecord>>
        implements RuntimableRuntime<AggregateProperties> {

    private AggregateProperties properties;

    private Set<String> groupColPathList = new HashSet<>();

    private Set<String> funcColPathList = new HashSet<>();

    @Override
    public PCollection<IndexedRecord> expand(PCollection<IndexedRecord> indexedRecordPCollection) {
        PCollection<KV<IndexedRecord, IndexedRecord>> kv = indexedRecordPCollection
                .apply(ParDo.of(new ExtractKVFn(new ArrayList<>(groupColPathList), new ArrayList<>(funcColPathList))))
                .setCoder(KvCoder.of(LazyAvroCoder.of(), LazyAvroCoder.of()));

        PCollection<KV<IndexedRecord, IndexedRecord>> aggregateResult = kv
                .apply(Combine.<IndexedRecord, IndexedRecord, IndexedRecord> perKey(new AggregateCombineFn(properties)))
                .setCoder(KvCoder.of(LazyAvroCoder.of(), LazyAvroCoder.of()));

        PCollection<IndexedRecord> result =
                aggregateResult.apply(ParDo.of(new MergeKVFn())).setCoder(LazyAvroCoder.of());

        return result;
    }

    @Override
    public ValidationResult initialize(RuntimeContainer container, AggregateProperties properties) {
        this.properties = properties;
        for (AggregateGroupProperties groupProps : properties.filteredGroupBy()) {
            groupColPathList.add(groupProps.columnName.getValue());
        }
        for (AggregateFunctionProperties funcProps : properties.filteredFunctions()) {
            funcColPathList.add(funcProps.columnName.getValue());
        }

        return ValidationResult.OK;
    }
}

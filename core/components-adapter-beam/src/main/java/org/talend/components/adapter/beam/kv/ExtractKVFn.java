// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.adapter.beam.kv;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ExtractKVFn extends DoFn<IndexedRecord, KV<IndexedRecord, IndexedRecord>> {

    private List<String> keyList = null;

    private transient Schema keySchema = null;

    private transient Schema valueSchema = null;

    public ExtractKVFn(List<String> keyList) {
        this.keyList = keyList;
    }

    @Setup
    public void setup() throws Exception {
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        IndexedRecord inputRecord = context.element();
        if (keySchema == null) {
            keySchema = SchemaGeneratorUtils.extractKeys(inputRecord.getSchema(), keyList);
        }
        if (valueSchema == null) {
            valueSchema = SchemaGeneratorUtils.extractValues(inputRecord.getSchema(), keyList);
        }
        context.output(KV.of(KeyValueUtils.extractIndexedRecord(inputRecord, keySchema),
                KeyValueUtils.extractIndexedRecord(inputRecord, valueSchema)));
    }

}

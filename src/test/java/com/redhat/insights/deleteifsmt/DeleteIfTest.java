/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redhat.insights.deleteifsmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class DeleteIfTest {
    private DeleteIf<SinkRecord> xform = new DeleteIf.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        xform.configure(new HashMap<>());

        final Map<String, Object> value = new HashMap<>();
        value.put("name", "Josef");
        value.put("age", 42);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);
        assertNull(transformedRecord);
    }

    @Test
    public void doNotDelete() {
        final Map<String, String> props = new HashMap<>();
        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("address", Schema.STRING_SCHEMA)
                .field("__deleted", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("address","Studenec");
        value.put("__deleted","false");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(2, updatedValue.schema().fields().size());
        assertEquals("Studenec", updatedValue.getString("address"));
        assertEquals("false", updatedValue.getString("__deleted"));
    }

    @Test
    public void doDelete() {
        final Map<String, String> props = new HashMap<>();
        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("address", Schema.STRING_SCHEMA)
                .field("__deleted", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("address","Studenec");
        value.put("__deleted","true");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertNull(updatedValue);
    }
}

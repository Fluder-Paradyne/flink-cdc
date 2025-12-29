/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.debezium.event;

import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DebeziumEventDeserializationSchema}. */
public class DebeziumEventDeserializationSchemaTest {

    @Test
    public void testConvertToLocalTimeZoneTimestampWithUTC() throws Exception {
        TestDeserializationSchema schema = new TestDeserializationSchema();

        // Test UTC format (Z suffix)
        String utcTimestamp = "2020-07-17T18:00:22Z";
        LocalZonedTimestampData result =
                (LocalZonedTimestampData)
                        schema.convertToLocalTimeZoneTimestamp(utcTimestamp, null);

        Instant expected = Instant.parse(utcTimestamp);
        assertThat(result.toInstant()).isEqualTo(expected);
    }

    @Test
    public void testConvertToLocalTimeZoneTimestampWithPositiveOffset() throws Exception {
        TestDeserializationSchema schema = new TestDeserializationSchema();

        // Test positive timezone offset (+08:00)
        String timestampWithOffset = "2020-07-17T18:00:22+08:00";
        LocalZonedTimestampData result =
                (LocalZonedTimestampData)
                        schema.convertToLocalTimeZoneTimestamp(timestampWithOffset, null);

        // The instant should be 2020-07-17 10:00:22 UTC (18:00:22+08:00 = 10:00:22 UTC)
        Instant expected = Instant.parse("2020-07-17T10:00:22Z");
        assertThat(result.toInstant()).isEqualTo(expected);
    }

    @Test
    public void testConvertToLocalTimeZoneTimestampWithNegativeOffset() throws Exception {
        TestDeserializationSchema schema = new TestDeserializationSchema();

        // Test negative timezone offset (-05:00)
        String timestampWithOffset = "2020-07-17T18:00:22-05:00";
        LocalZonedTimestampData result =
                (LocalZonedTimestampData)
                        schema.convertToLocalTimeZoneTimestamp(timestampWithOffset, null);

        // The instant should be 2020-07-17 23:00:22 UTC (18:00:22-05:00 = 23:00:22 UTC)
        Instant expected = Instant.parse("2020-07-17T23:00:22Z");
        assertThat(result.toInstant()).isEqualTo(expected);
    }

    @Test
    public void testConvertToLocalTimeZoneTimestampWithMicroseconds() throws Exception {
        TestDeserializationSchema schema = new TestDeserializationSchema();

        // Test with microsecond precision
        String timestampWithMicros = "2020-07-17T18:00:22.123456+08:00";
        LocalZonedTimestampData result =
                (LocalZonedTimestampData)
                        schema.convertToLocalTimeZoneTimestamp(timestampWithMicros, null);

        // The instant should be 2020-07-17 10:00:22.123456 UTC
        Instant expected = Instant.parse("2020-07-17T10:00:22.123456Z");
        assertThat(result.toInstant()).isEqualTo(expected);
    }

    @Test
    public void testConvertToLocalTimeZoneTimestampWithZeroOffset() throws Exception {
        TestDeserializationSchema schema = new TestDeserializationSchema();

        // Test with +00:00 offset (equivalent to Z)
        String timestampWithZeroOffset = "2020-07-17T18:00:22+00:00";
        LocalZonedTimestampData result =
                (LocalZonedTimestampData)
                        schema.convertToLocalTimeZoneTimestamp(timestampWithZeroOffset, null);

        Instant expected = Instant.parse("2020-07-17T18:00:22Z");
        assertThat(result.toInstant()).isEqualTo(expected);
    }

    /** Test implementation to access protected method. */
    private static class TestDeserializationSchema extends DebeziumEventDeserializationSchema {

        public TestDeserializationSchema() {
            super(new DebeziumSchemaDataTypeInference(), DebeziumChangelogMode.ALL);
        }

        @Override
        public Object convertToLocalTimeZoneTimestamp(
                Object dbzObj, org.apache.kafka.connect.data.Schema schema) {
            return super.convertToLocalTimeZoneTimestamp(dbzObj, schema);
        }

        @Override
        public boolean isDataChangeRecord(SourceRecord record) {
            return false;
        }

        @Override
        public boolean isSchemaChangeRecord(SourceRecord record) {
            return false;
        }

        @Override
        public TableId getTableId(SourceRecord record) {
            return null;
        }

        @Override
        public Map<String, String> getMetadata(SourceRecord record) {
            return Collections.emptyMap();
        }

        @Override
        public List<DataChangeEvent> deserializeDataChangeRecord(SourceRecord record) {
            return Collections.emptyList();
        }

        @Override
        public List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
            return Collections.emptyList();
        }
    }
}

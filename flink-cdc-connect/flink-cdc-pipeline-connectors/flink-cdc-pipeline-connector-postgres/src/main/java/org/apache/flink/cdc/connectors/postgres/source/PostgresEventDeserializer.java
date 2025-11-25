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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.postgres.table.PostgreSQLReadableMetadata;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKBReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Event deserializer for {@link PostgresDataSource}. */
@Internal
public class PostgresEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresEventDeserializer.class);

    private static final long serialVersionUID = 1L;
    private List<PostgreSQLReadableMetadata> readableMetadataList;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Helper method to convert bytes to hex string for logging */
    private static String bytesToHex(byte[] bytes, int maxLength) {
        StringBuilder sb = new StringBuilder();
        int len = Math.min(bytes.length, maxLength);
        for (int i = 0; i < len; i++) {
            sb.append(String.format("%02X ", bytes[i] & 0xFF));
        }
        if (bytes.length > maxLength) {
            sb.append("...");
        }
        return sb.toString();
    }

    public PostgresEventDeserializer(DebeziumChangelogMode changelogMode) {
        super(new PostgresSchemaDataTypeInference(), changelogMode);
    }

    public PostgresEventDeserializer(
            DebeziumChangelogMode changelogMode,
            List<PostgreSQLReadableMetadata> readableMetadataList) {
        super(new PostgresSchemaDataTypeInference(), changelogMode);
        this.readableMetadataList = readableMetadataList;
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        return Collections.emptyList();
    }

    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return value != null
                && valueSchema != null
                && valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    @Override
    protected boolean isSchemaChangeRecord(SourceRecord record) {
        return false;
    }

    @Override
    protected TableId getTableId(SourceRecord record) {
        String[] parts = record.topic().split("\\.");
        return TableId.tableId(parts[1], parts[2]);
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> metadataMap = new HashMap<>();
        readableMetadataList.forEach(
                (postgresReadableMetadata -> {
                    Object metadata = postgresReadableMetadata.getConverter().read(record);
                    if (postgresReadableMetadata.equals(PostgreSQLReadableMetadata.OP_TS)) {
                        metadataMap.put(
                                postgresReadableMetadata.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        metadataMap.put(
                                postgresReadableMetadata.getKey(), String.valueOf(metadata));
                    }
                }));
        return metadataMap;
    }

    @Override
    protected Object convertToString(Object dbzObj, Schema schema) {
        // the Geometry datatype in PostgreSQL will be converted to
        // a String with Json format
        if (Point.LOGICAL_NAME.equals(schema.name())
                || Geometry.LOGICAL_NAME.equals(schema.name())
                || Geography.LOGICAL_NAME.equals(schema.name())) {
            try {
                Struct geometryStruct = (Struct) dbzObj;
                byte[] wkb = geometryStruct.getBytes("wkb");

                WKBReader wkbReader = new WKBReader();
                org.locationtech.jts.geom.Geometry jtsGeom = wkbReader.read(wkb);

                Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
                Map<String, Object> geometryInfo = new HashMap<>();
                String geometryType = jtsGeom.getGeometryType();
                geometryInfo.put("type", geometryType);

                if (geometryType.equals("GeometryCollection")) {
                    geometryInfo.put("geometries", jtsGeom.toText());
                } else {
                    Coordinate[] coordinates = jtsGeom.getCoordinates();
                    List<double[]> coordinateList = new ArrayList<>();
                    if (coordinates != null) {
                        for (Coordinate coordinate : coordinates) {
                            coordinateList.add(new double[] {coordinate.x, coordinate.y});
                            geometryInfo.put(
                                    "coordinates", new double[] {coordinate.x, coordinate.y});
                        }
                    }
                    geometryInfo.put(
                            "coordinates", OBJECT_MAPPER.writeValueAsString(coordinateList));
                }
                geometryInfo.put("srid", srid.orElse(0));
                return BinaryStringData.fromString(OBJECT_MAPPER.writeValueAsString(geometryInfo));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to convert %s to geometry JSON.", dbzObj), e);
            }
        } else {
            // Handle byte[] arrays (e.g., JSONB binary data from PostgreSQL 17)
            // PostgreSQL 17 may send JSONB as binary data that needs to be decoded as UTF-8
            if (dbzObj instanceof byte[]) {
                byte[] bytes = (byte[]) dbzObj;
                
                // Log deployment verification - this confirms the patched code is running
                LOG.info(
                        "[PATCHED CODE] PostgresEventDeserializer.convertToString detected byte[] input "
                                + "(likely JSONB from PostgreSQL 17). Schema name: {}, Schema type: {}, Bytes length: {}, "
                                + "First byte: 0x{}, First 50 bytes hex: {}",
                        schema != null ? schema.name() : "null",
                        schema != null && schema.type() != null ? schema.type().toString() : "null",
                        bytes.length,
                        bytes.length > 0 ? String.format("%02X", bytes[0] & 0xFF) : "N/A",
                        bytesToHex(bytes, 50));
                
                // Try to decode as UTF-8 string, which is the standard encoding for JSONB text
                // representation in PostgreSQL
                try {
                    // PostgreSQL JSONB binary format: version byte (0x01) + JSON text in UTF-8
                    // Try decoding from the start first (in case Debezium already stripped the version byte)
                    String jsonbStr = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                    // Validate it looks like JSON (starts with { or [)
                    if (jsonbStr.trim().startsWith("{") || jsonbStr.trim().startsWith("[")) {
                        LOG.debug(
                                "[PATCHED CODE] Successfully decoded JSONB from byte[] (direct UTF-8). "
                                        + "Length: {}, Preview: {}",
                                jsonbStr.length(),
                                jsonbStr.length() > 100 ? jsonbStr.substring(0, 100) + "..." : jsonbStr);
                        return BinaryStringData.fromString(jsonbStr);
                    }
                    // If not valid JSON, try skipping version byte
                    if (bytes.length > 0 && bytes[0] == 0x01) {
                        LOG.debug(
                                "[PATCHED CODE] Detected version byte 0x01, attempting to decode with version byte skipped");
                        jsonbStr = new String(bytes, 1, bytes.length - 1, java.nio.charset.StandardCharsets.UTF_8);
                        if (jsonbStr.trim().startsWith("{") || jsonbStr.trim().startsWith("[")) {
                            LOG.debug(
                                    "[PATCHED CODE] Successfully decoded JSONB from byte[] (skipped version byte). "
                                            + "Length: {}, Preview: {}",
                                    jsonbStr.length(),
                                    jsonbStr.length() > 100 ? jsonbStr.substring(0, 100) + "..." : jsonbStr);
                            return BinaryStringData.fromString(jsonbStr);
                        }
                    }
                    // If still not valid, return as-is (might be corrupted or different format)
                    LOG.warn(
                            "[PATCHED CODE] Decoded byte[] but result doesn't look like JSON. "
                                    + "Returning as-is. Length: {}, Preview: {}",
                            jsonbStr.length(),
                            jsonbStr.length() > 100 ? jsonbStr.substring(0, 100) + "..." : jsonbStr);
                    return BinaryStringData.fromString(jsonbStr);
                } catch (Exception e) {
                    // Fallback to base64 encoding if UTF-8 decoding fails
                    LOG.warn(
                            "[PATCHED CODE] Failed to decode byte[] as UTF-8, falling back to base64 encoding. "
                                    + "Error: {}",
                            e.getMessage());
                    return BinaryStringData.fromString(
                            java.util.Base64.getEncoder().encodeToString(bytes));
                }
            } else if (dbzObj instanceof java.nio.ByteBuffer) {
                java.nio.ByteBuffer byteBuffer = (java.nio.ByteBuffer) dbzObj;
                byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                
                LOG.info(
                        "[PATCHED CODE] PostgresEventDeserializer.convertToString detected ByteBuffer input "
                                + "(likely JSONB from PostgreSQL 17). Schema: {}, Bytes length: {}",
                        schema != null ? schema.name() : "null",
                        bytes.length);
                
                try {
                    String jsonbStr = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                    if (jsonbStr.trim().startsWith("{") || jsonbStr.trim().startsWith("[")) {
                        LOG.debug(
                                "[PATCHED CODE] Successfully decoded JSONB from ByteBuffer (direct UTF-8). "
                                        + "Length: {}, Preview: {}",
                                jsonbStr.length(),
                                jsonbStr.length() > 100 ? jsonbStr.substring(0, 100) + "..." : jsonbStr);
                        return BinaryStringData.fromString(jsonbStr);
                    }
                    if (bytes.length > 0 && bytes[0] == 0x01) {
                        LOG.debug(
                                "[PATCHED CODE] Detected version byte 0x01 in ByteBuffer, attempting to decode with version byte skipped");
                        jsonbStr = new String(bytes, 1, bytes.length - 1, java.nio.charset.StandardCharsets.UTF_8);
                        if (jsonbStr.trim().startsWith("{") || jsonbStr.trim().startsWith("[")) {
                            LOG.debug(
                                    "[PATCHED CODE] Successfully decoded JSONB from ByteBuffer (skipped version byte). "
                                            + "Length: {}, Preview: {}",
                                    jsonbStr.length(),
                                    jsonbStr.length() > 100 ? jsonbStr.substring(0, 100) + "..." : jsonbStr);
                            return BinaryStringData.fromString(jsonbStr);
                        }
                    }
                    LOG.warn(
                            "[PATCHED CODE] Decoded ByteBuffer but result doesn't look like JSON. "
                                    + "Returning as-is. Length: {}, Preview: {}",
                            jsonbStr.length(),
                            jsonbStr.length() > 100 ? jsonbStr.substring(0, 100) + "..." : jsonbStr);
                    return BinaryStringData.fromString(jsonbStr);
                } catch (Exception e) {
                    LOG.warn(
                            "[PATCHED CODE] Failed to decode ByteBuffer as UTF-8, falling back to base64 encoding. "
                                    + "Error: {}",
                            e.getMessage());
                    return BinaryStringData.fromString(
                            java.util.Base64.getEncoder().encodeToString(bytes));
                }
            } else if (dbzObj instanceof String) {
                // Handle String that might contain JSONB binary data (PostgreSQL 17 may send JSONB as String with binary chars)
                String str = (String) dbzObj;
                
                // Check if string contains binary characters and JSON-like content
                // This happens when JSONB binary data is converted to String incorrectly
                boolean hasBinaryChars = false;
                boolean hasJsonContent = false;
                
                for (int i = 0; i < Math.min(str.length(), 200); i++) {
                    char c = str.charAt(i);
                    if (Character.isISOControl(c) && c != '\n' && c != '\r' && c != '\t') {
                        hasBinaryChars = true;
                    }
                    if (c == '{' || c == '[') {
                        hasJsonContent = true;
                        break;
                    }
                }
                
                if (hasBinaryChars && hasJsonContent) {
                    LOG.info(
                            "[PATCHED CODE] PostgresEventDeserializer.convertToString detected String with binary characters and JSON content "
                                    + "(likely JSONB from PostgreSQL 17 that was incorrectly converted). "
                                    + "Schema name: {}, String length: {}, Preview: {}",
                            schema != null ? schema.name() : "null",
                            str.length(),
                            str.length() > 100 ? str.substring(0, 100) + "..." : str);
                    
                    // Try to extract JSON part by finding the first { or [
                    int jsonStart = -1;
                    for (int i = 0; i < str.length(); i++) {
                        char c = str.charAt(i);
                        if (c == '{' || c == '[') {
                            jsonStart = i;
                            break;
                        }
                    }
                    
                    if (jsonStart >= 0) {
                        String jsonPart = str.substring(jsonStart);
                        // Validate it looks like JSON
                        if (jsonPart.trim().startsWith("{") || jsonPart.trim().startsWith("[")) {
                            LOG.debug(
                                    "[PATCHED CODE] Extracted JSON from String with binary prefix. JSON length: {}, Preview: {}",
                                    jsonPart.length(),
                                    jsonPart.length() > 100 ? jsonPart.substring(0, 100) + "..." : jsonPart);
                            return BinaryStringData.fromString(jsonPart);
                        }
                    }
                    
                    // If extraction failed, return cleaned version (remove null bytes and control chars)
                    String cleaned = str.replaceAll("[\u0000-\u0008\u000B\u000C\u000E-\u001F]", "");
                    LOG.warn(
                            "[PATCHED CODE] Could not extract clean JSON from String. Returning cleaned version. "
                                    + "Original length: {}, Cleaned length: {}",
                            str.length(), cleaned.length());
                    return BinaryStringData.fromString(cleaned);
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(
                                "[PATCHED CODE] PostgresEventDeserializer.convertToString processing String object (no binary chars detected): {}",
                                str.length() > 50 ? str.substring(0, 50) + "..." : str);
                    }
                    return BinaryStringData.fromString(str);
                }
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "[PATCHED CODE] PostgresEventDeserializer.convertToString handling non-binary object. "
                                    + "Type: {}, Schema: {}",
                            dbzObj != null ? dbzObj.getClass().getName() : "null",
                            schema != null ? schema.name() : "null");
                }
                return BinaryStringData.fromString(dbzObj.toString());
            }
        }
    }
}

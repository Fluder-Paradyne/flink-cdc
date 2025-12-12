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

package org.apache.flink.cdc.connectors.clickhouse.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseUtils.createFieldGetter;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseUtils.quoteIdentifier;

/** Sink function for writing events to ClickHouse. */
public class ClickHouseDataSinkFunction extends RichSinkFunction<Event> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseDataSinkFunction.class);

    private final String url;
    private final String username;
    private final String password;
    private final String databaseName;
    private final int batchSize;
    private final long flushInterval;
    private final ZoneId zoneId;

    private transient Connection connection;
    private transient Map<TableId, TableInfo> tableInfoMap;
    private transient Map<TableId, List<Object[]>> batchBuffer;
    private transient Map<TableId, PreparedStatement> preparedStatements;
    private transient long lastFlushTime;

    public ClickHouseDataSinkFunction(
            String url,
            String username,
            String password,
            String databaseName,
            int batchSize,
            long flushInterval,
            ZoneId zoneId) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.zoneId = zoneId;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String jdbcUrl =
                url.startsWith("jdbc:")
                        ? url
                        : "jdbc:clickhouse://" + url.replace("http://", "").replace("https://", "");
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        connection = DriverManager.getConnection(jdbcUrl, properties);
        tableInfoMap = new HashMap<>();
        batchBuffer = new HashMap<>();
        preparedStatements = new HashMap<>();
        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        if (value instanceof SchemaChangeEvent) {
            applySchemaChangeEvent((SchemaChangeEvent) value);
        } else if (value instanceof DataChangeEvent) {
            applyDataChangeEvent((DataChangeEvent) value);
        } else {
            throw new UnsupportedOperationException("Don't support event " + value);
        }
    }

    private void applySchemaChangeEvent(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            TableInfo tableInfo = tableInfoMap.get(tableId);
            if (tableInfo == null) {
                throw new RuntimeException("schema of " + tableId + " is not existed.");
            }
            newSchema = SchemaUtils.applySchemaChangeEvent(tableInfo.schema, event);
        }
        TableInfo tableInfo = new TableInfo();
        tableInfo.schema = newSchema;
        tableInfo.fieldGetters = new RecordData.FieldGetter[newSchema.getColumnCount()];
        for (int i = 0; i < newSchema.getColumnCount(); i++) {
            tableInfo.fieldGetters[i] =
                    createFieldGetter(newSchema.getColumns().get(i).getType(), i, zoneId);
        }
        tableInfoMap.put(tableId, tableInfo);

        // Prepare INSERT statement for the table
        prepareInsertStatement(tableId, newSchema);
    }

    private void prepareInsertStatement(TableId tableId, Schema schema) {
        try {
            List<String> columnNames = new ArrayList<>();
            for (Column column : schema.getColumns()) {
                columnNames.add(column.getName());
            }

            String insertSQL =
                    "INSERT INTO "
                            + quoteIdentifier(tableId.getSchemaName())
                            + "."
                            + quoteIdentifier(tableId.getTableName())
                            + " ("
                            + String.join(
                                    ", ",
                                    columnNames.stream()
                                            .map(ClickHouseUtils::quoteIdentifier)
                                            .collect(java.util.stream.Collectors.toList()))
                            + ") VALUES ("
                            + String.join(
                                    ", ",
                                    columnNames.stream()
                                            .map(c -> "?")
                                            .collect(java.util.stream.Collectors.toList()))
                            + ")";

            PreparedStatement ps = connection.prepareStatement(insertSQL);
            preparedStatements.put(tableId, ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to prepare insert statement for " + tableId, e);
        }
    }

    private void applyDataChangeEvent(DataChangeEvent event) throws Exception {
        TableId tableId = event.tableId();
        TableInfo tableInfo = tableInfoMap.get(tableId);
        Preconditions.checkNotNull(tableInfo, tableId + " is not existed");

        Object[] rowData;
        switch (event.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                rowData = extractRowData(tableInfo, event.after());
                break;
            case DELETE:
                // For DELETE, we need to handle it based on ClickHouse capabilities
                // For now, we'll skip DELETE events or implement a DELETE mechanism
                LOG.warn("DELETE operation is not fully supported, skipping event: {}", event);
                return;
            default:
                throw new UnsupportedOperationException(
                        "Don't support operation type " + event.op());
        }

        // Add to batch buffer
        batchBuffer.computeIfAbsent(tableId, k -> new ArrayList<>()).add(rowData);

        // Flush if batch size reached or flush interval elapsed
        if (shouldFlush(tableId)) {
            flush(tableId);
        }
    }

    private Object[] extractRowData(TableInfo tableInfo, RecordData record) {
        Object[] rowData = new Object[record.getArity()];
        for (int i = 0; i < record.getArity(); i++) {
            rowData[i] = tableInfo.fieldGetters[i].getFieldOrNull(record);
        }
        return rowData;
    }

    private boolean shouldFlush(TableId tableId) {
        List<Object[]> buffer = batchBuffer.get(tableId);
        if (buffer == null || buffer.isEmpty()) {
            return false;
        }
        return buffer.size() >= batchSize
                || (System.currentTimeMillis() - lastFlushTime) >= flushInterval;
    }

    private void flush(TableId tableId) throws SQLException {
        List<Object[]> buffer = batchBuffer.get(tableId);
        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        PreparedStatement ps = preparedStatements.get(tableId);
        if (ps == null) {
            LOG.warn("No prepared statement for table {}, skipping flush", tableId);
            return;
        }

        try {
            connection.setAutoCommit(false);
            for (Object[] rowData : buffer) {
                for (int i = 0; i < rowData.length; i++) {
                    ps.setObject(i + 1, rowData[i]);
                }
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();
            LOG.debug("Flushed {} rows to table {}", buffer.size(), tableId);
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();
        }
    }

    @Override
    public void close() throws Exception {
        // Flush remaining data
        for (TableId tableId : batchBuffer.keySet()) {
            if (!batchBuffer.get(tableId).isEmpty()) {
                flush(tableId);
            }
        }

        // Close prepared statements
        for (PreparedStatement ps : preparedStatements.values()) {
            if (ps != null) {
                ps.close();
            }
        }

        // Close connection
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        super.close();
    }

    /** Table information. */
    private static class TableInfo {
        Schema schema;
        RecordData.FieldGetter[] fieldGetters;
    }
}

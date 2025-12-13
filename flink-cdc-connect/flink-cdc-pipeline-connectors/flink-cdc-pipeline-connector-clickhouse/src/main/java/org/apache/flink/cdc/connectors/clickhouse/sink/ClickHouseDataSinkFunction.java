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
    private transient Map<TableId, List<OperationData>> batchBuffer;
    private transient Map<TableId, PreparedStatement> insertStatements;
    private transient Map<TableId, PreparedStatement> deleteStatements;
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
        // Explicitly load ClickHouse driver (service file excluded from JAR to avoid conflicts)
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load ClickHouse JDBC driver", e);
        }
        // Build JDBC URL with HTTP protocol and disable compression
        String jdbcUrl;
        if (url.startsWith("jdbc:")) {
            jdbcUrl = url;
        } else {
            // Convert http://host:port to jdbc:clickhouse:http://host:port
            String cleanUrl = url.replace("http://", "").replace("https://", "");
            jdbcUrl = "jdbc:clickhouse:http://" + cleanUrl + "/" + databaseName;
        }
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        // Disable compression to avoid protocol mismatch issues
        properties.setProperty("compress", "false");
        properties.setProperty("decompress", "false");
        connection = DriverManager.getConnection(jdbcUrl, properties);
        tableInfoMap = new HashMap<>();
        batchBuffer = new HashMap<>();
        insertStatements = new HashMap<>();
        deleteStatements = new HashMap<>();
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

        // Prepare INSERT and DELETE statements for the table
        prepareStatements(tableId, newSchema);
    }

    private void prepareStatements(TableId tableId, Schema schema) {
        try {
            List<String> columnNames = new ArrayList<>();
            for (Column column : schema.getColumns()) {
                columnNames.add(column.getName());
            }

            // Prepare INSERT statement
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

            PreparedStatement insertPs = connection.prepareStatement(insertSQL);
            insertStatements.put(tableId, insertPs);

            // Prepare DELETE statement if table has primary keys
            if (!schema.primaryKeys().isEmpty()) {
                // DELETE statement will be built dynamically based on primary keys
                // We'll use ALTER TABLE DELETE WHERE for better performance
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to prepare statements for " + tableId, e);
        }
    }

    private void applyDataChangeEvent(DataChangeEvent event) throws Exception {
        TableId tableId = event.tableId();
        TableInfo tableInfo = tableInfoMap.get(tableId);
        Preconditions.checkNotNull(tableInfo, tableId + " is not existed");

        OperationData operationData;
        switch (event.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                // For UPDATE: With ReplacingMergeTree, we insert the new row
                // ClickHouse will automatically replace the old row with the same primary key
                Object[] rowData = extractRowData(tableInfo, event.after());
                operationData = new OperationData(OperationType.INSERT, rowData, null);
                break;
            case DELETE:
                // For DELETE: Use ALTER TABLE DELETE WHERE with primary key conditions
                RecordData beforeRecord = event.before();
                operationData = new OperationData(OperationType.DELETE, null, beforeRecord);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Don't support operation type " + event.op());
        }

        // Add to batch buffer
        batchBuffer.computeIfAbsent(tableId, k -> new ArrayList<>()).add(operationData);

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
        List<OperationData> buffer = batchBuffer.get(tableId);
        if (buffer == null || buffer.isEmpty()) {
            return false;
        }
        return buffer.size() >= batchSize
                || (System.currentTimeMillis() - lastFlushTime) >= flushInterval;
    }

    private void flush(TableId tableId) throws SQLException {
        List<OperationData> buffer = batchBuffer.get(tableId);
        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        TableInfo tableInfo = tableInfoMap.get(tableId);
        if (tableInfo == null) {
            LOG.warn("No table info for table {}, skipping flush", tableId);
            return;
        }

        PreparedStatement insertPs = insertStatements.get(tableId);
        if (insertPs == null) {
            LOG.warn("No insert statement for table {}, skipping flush", tableId);
            return;
        }

        try {
            connection.setAutoCommit(false);

            // Separate INSERT and DELETE operations
            List<Object[]> insertBatch = new ArrayList<>();
            List<RecordData> deleteBatch = new ArrayList<>();

            for (OperationData opData : buffer) {
                if (opData.operationType == OperationType.INSERT) {
                    insertBatch.add(opData.rowData);
                } else if (opData.operationType == OperationType.DELETE) {
                    deleteBatch.add(opData.beforeRecord);
                }
            }

            // Execute INSERT operations
            if (!insertBatch.isEmpty()) {
                for (Object[] rowData : insertBatch) {
                    for (int i = 0; i < rowData.length; i++) {
                        insertPs.setObject(i + 1, rowData[i]);
                    }
                    insertPs.addBatch();
                }
                insertPs.executeBatch();
            }

            // Execute DELETE operations using ALTER TABLE DELETE WHERE
            if (!deleteBatch.isEmpty()) {
                executeDeletes(tableId, tableInfo, deleteBatch);
            }

            connection.commit();
            LOG.debug(
                    "Flushed {} operations ({} inserts, {} deletes) to table {}",
                    buffer.size(),
                    insertBatch.size(),
                    deleteBatch.size(),
                    tableId);
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();
        }
    }

    private void executeDeletes(
            TableId tableId, TableInfo tableInfo, List<RecordData> deleteRecords)
            throws SQLException {
        if (tableInfo.schema.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "DELETE operations require primary keys, but table "
                            + tableId
                            + " has no primary keys");
        }

        try (java.sql.Statement stmt = connection.createStatement()) {
            for (RecordData record : deleteRecords) {
                String whereClause =
                        ClickHouseUtils.buildPrimaryKeyWhereClause(
                                tableInfo.schema, record, zoneId);
                String deleteSQL =
                        "ALTER TABLE "
                                + quoteIdentifier(tableId.getSchemaName())
                                + "."
                                + quoteIdentifier(tableId.getTableName())
                                + " DELETE WHERE "
                                + whereClause;
                stmt.execute(deleteSQL);
            }
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
        for (PreparedStatement ps : insertStatements.values()) {
            if (ps != null) {
                ps.close();
            }
        }
        for (PreparedStatement ps : deleteStatements.values()) {
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

    /** Operation type for batch processing. */
    private enum OperationType {
        INSERT,
        DELETE
    }

    /** Operation data holder. */
    private static class OperationData {
        final OperationType operationType;
        final Object[] rowData; // For INSERT operations
        final RecordData beforeRecord; // For DELETE operations

        OperationData(OperationType operationType, Object[] rowData, RecordData beforeRecord) {
            this.operationType = operationType;
            this.rowData = rowData;
            this.beforeRecord = beforeRecord;
        }
    }
}

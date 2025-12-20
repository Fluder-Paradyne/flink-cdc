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
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.IS_DELETED_COLUMN;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.IS_DELETED_FALSE;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.IS_DELETED_TRUE;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.VERSION_COLUMN;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseUtils.createFieldGetter;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseUtils.quoteIdentifier;

/**
 * Sink function for writing CDC events to ClickHouse using ReplacingMergeTree.
 *
 * <p>This implementation uses ReplacingMergeTree with version and is_deleted columns to handle CDC
 * operations:
 *
 * <ul>
 *   <li><b>INSERT</b>: Insert row with _version=N, _is_deleted=0
 *   <li><b>UPDATE</b>: Insert new row with _version=N+1, _is_deleted=0 (old row replaced on merge)
 *   <li><b>DELETE</b>: Insert row with _version=N+1, _is_deleted=1 (soft delete, filtered by FINAL)
 * </ul>
 *
 * <p>Benefits of this approach:
 *
 * <ul>
 *   <li>No mutations (ALTER TABLE DELETE) which are heavy operations
 *   <li>Append-only writes for high throughput
 *   <li>Consistent results with SELECT * FROM table FINAL
 * </ul>
 */
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
    private transient Map<TableId, List<RowData>> batchBuffer;
    private transient Map<TableId, PreparedStatement> insertStatements;
    private transient long lastFlushTime;
    private transient VersionGenerator versionGenerator;

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

        // Initialize version generator with subtask ID for uniqueness across parallel instances
        int subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        versionGenerator = new VersionGenerator(subtaskId);
        LOG.info(
                "Initialized VersionGenerator for subtask {} (parallelism: {})",
                subtaskId,
                getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks());

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
        tableInfo.hasPrimaryKey = !newSchema.primaryKeys().isEmpty();
        tableInfo.fieldGetters = new RecordData.FieldGetter[newSchema.getColumnCount()];
        for (int i = 0; i < newSchema.getColumnCount(); i++) {
            tableInfo.fieldGetters[i] =
                    createFieldGetter(newSchema.getColumns().get(i).getType(), i, zoneId);
        }
        tableInfoMap.put(tableId, tableInfo);

        // Prepare INSERT statement for the table
        prepareInsertStatement(tableId, newSchema, tableInfo.hasPrimaryKey);
    }

    private void prepareInsertStatement(TableId tableId, Schema schema, boolean hasPrimaryKey) {
        try {
            // Close existing statement if any
            PreparedStatement existingPs = insertStatements.get(tableId);
            if (existingPs != null) {
                existingPs.close();
            }

            // Build column list (user columns + system columns for tables with PK)
            List<String> columnNames = new ArrayList<>();
            for (Column column : schema.getColumns()) {
                columnNames.add(column.getName());
            }

            // Add system columns for CDC if table has primary key
            if (hasPrimaryKey) {
                columnNames.add(VERSION_COLUMN);
                columnNames.add(IS_DELETED_COLUMN);
            }

            // Build INSERT statement
            String insertSQL =
                    "INSERT INTO "
                            + quoteIdentifier(tableId.getSchemaName())
                            + "."
                            + quoteIdentifier(tableId.getTableName())
                            + " ("
                            + columnNames.stream()
                                    .map(ClickHouseUtils::quoteIdentifier)
                                    .collect(Collectors.joining(", "))
                            + ") VALUES ("
                            + columnNames.stream().map(c -> "?").collect(Collectors.joining(", "))
                            + ")";

            PreparedStatement insertPs = connection.prepareStatement(insertSQL);
            insertStatements.put(tableId, insertPs);

            LOG.debug("Prepared INSERT statement for {}: {}", tableId, insertSQL);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to prepare statements for " + tableId, e);
        }
    }

    private void applyDataChangeEvent(DataChangeEvent event) throws Exception {
        TableId tableId = event.tableId();
        TableInfo tableInfo = tableInfoMap.get(tableId);
        Preconditions.checkNotNull(tableInfo, tableId + " is not existed");

        RowData rowData;
        switch (event.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                // INSERT and UPDATE: Insert row with _is_deleted=0
                // For UPDATE, ReplacingMergeTree will keep the row with highest _version
                Object[] insertData = extractRowData(tableInfo, event.after());
                rowData = new RowData(insertData, IS_DELETED_FALSE);
                break;

            case DELETE:
                // DELETE: Insert row with _is_deleted=1 (soft delete)
                // ReplacingMergeTree with is_deleted will filter these during FINAL queries
                Object[] deleteData = extractRowData(tableInfo, event.before());
                rowData = new RowData(deleteData, IS_DELETED_TRUE);
                break;

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
        List<RowData> buffer = batchBuffer.get(tableId);
        if (buffer == null || buffer.isEmpty()) {
            return false;
        }
        return buffer.size() >= batchSize
                || (System.currentTimeMillis() - lastFlushTime) >= flushInterval;
    }

    private void flush(TableId tableId) throws SQLException {
        List<RowData> buffer = batchBuffer.get(tableId);
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

            for (RowData rowData : buffer) {
                int paramIndex = 1;

                // Set user column values
                for (Object value : rowData.columnValues) {
                    insertPs.setObject(paramIndex++, value);
                }

                // Set system columns for tables with primary key
                if (tableInfo.hasPrimaryKey) {
                    // _version: unique, monotonically increasing
                    long version = versionGenerator.nextVersion();
                    insertPs.setLong(paramIndex++, version);

                    // _is_deleted: 0 for active rows, 1 for deleted rows
                    insertPs.setInt(paramIndex++, rowData.isDeleted);
                }

                insertPs.addBatch();
            }

            insertPs.executeBatch();
            connection.commit();

            LOG.debug(
                    "Flushed {} rows to table {} (subtask {})",
                    buffer.size(),
                    tableId,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
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
        if (batchBuffer != null) {
            for (TableId tableId : batchBuffer.keySet()) {
                if (!batchBuffer.get(tableId).isEmpty()) {
                    flush(tableId);
                }
            }
        }

        // Close prepared statements
        if (insertStatements != null) {
            for (PreparedStatement ps : insertStatements.values()) {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        LOG.warn("Error closing prepared statement", e);
                    }
                }
            }
        }

        // Close connection
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        super.close();
    }

    /** Table information including schema and field getters. */
    private static class TableInfo {
        Schema schema;
        boolean hasPrimaryKey;
        RecordData.FieldGetter[] fieldGetters;
    }

    /** Row data holder with column values and delete flag. */
    private static class RowData {
        final Object[] columnValues;
        final int isDeleted;

        RowData(Object[] columnValues, int isDeleted) {
            this.columnValues = columnValues;
            this.isDeleted = isDeleted;
        }
    }
}

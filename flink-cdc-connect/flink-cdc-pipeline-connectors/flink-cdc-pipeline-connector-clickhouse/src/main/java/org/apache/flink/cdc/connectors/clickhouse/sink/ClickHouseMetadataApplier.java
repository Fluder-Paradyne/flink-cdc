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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseUtils.generateCreateTableDDL;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseUtils.quoteIdentifier;

/** A {@code MetadataApplier} that applies metadata changes to ClickHouse. */
public class ClickHouseMetadataApplier implements MetadataApplier {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseMetadataApplier.class);

    private final String url;
    private final String username;
    private final String password;
    private final String databaseName;
    private final TableCreateConfig tableCreateConfig;
    private final SchemaChangeConfig schemaChangeConfig;
    private boolean isOpened;
    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;
    private transient Connection connection;

    public ClickHouseMetadataApplier(
            String url,
            String username,
            String password,
            String databaseName,
            TableCreateConfig tableCreateConfig,
            SchemaChangeConfig schemaChangeConfig) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableCreateConfig = tableCreateConfig;
        this.schemaChangeConfig = schemaChangeConfig;
        this.isOpened = false;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Sets.newHashSet(
                SchemaChangeEventType.CREATE_TABLE,
                SchemaChangeEventType.ADD_COLUMN,
                SchemaChangeEventType.DROP_COLUMN,
                SchemaChangeEventType.RENAME_COLUMN,
                SchemaChangeEventType.ALTER_COLUMN_TYPE,
                SchemaChangeEventType.DROP_TABLE,
                SchemaChangeEventType.TRUNCATE_TABLE);
    }

    private void ensureConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            String jdbcUrl =
                    url.startsWith("jdbc:")
                            ? url
                            : "jdbc:clickhouse://"
                                    + url.replace("http://", "").replace("https://", "");
            Properties properties = new Properties();
            properties.setProperty("user", username);
            properties.setProperty("password", password);
            // Set connection timeouts to prevent blocking coordinator requests
            // Use shorter timeout for schema changes (5 seconds) to avoid coordinator timeout
            properties.setProperty("connect_timeout", "5000"); // 5 seconds
            properties.setProperty("socket_timeout", "10000"); // 10 seconds
            properties.setProperty("connection_timeout", "5000"); // 5 seconds
            connection = DriverManager.getConnection(jdbcUrl, properties);
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (!isOpened) {
            isOpened = true;
            try {
                ensureConnection();
            } catch (SQLException e) {
                throw new SchemaEvolveException(
                        schemaChangeEvent, "Failed to open connection to ClickHouse", e);
            }
        }

        SchemaChangeEventVisitor.visit(
                schemaChangeEvent,
                addColumnEvent -> {
                    applyAddColumn(addColumnEvent);
                    return null;
                },
                alterColumnTypeEvent -> {
                    applyAlterColumnType(alterColumnTypeEvent);
                    return null;
                },
                createTableEvent -> {
                    applyCreateTable(createTableEvent);
                    return null;
                },
                dropColumnEvent -> {
                    applyDropColumn(dropColumnEvent);
                    return null;
                },
                dropTableEvent -> {
                    applyDropTable(dropTableEvent);
                    return null;
                },
                renameColumnEvent -> {
                    applyRenameColumn(renameColumnEvent);
                    return null;
                },
                truncateTableEvent -> {
                    applyTruncateTable(truncateTableEvent);
                    return null;
                });
    }

    private void applyCreateTable(CreateTableEvent createTableEvent) throws SchemaEvolveException {
        try {
            ensureConnection();
            Statement stmt = connection.createStatement();

            // Create database if not exists
            String createDbSQL = "CREATE DATABASE IF NOT EXISTS " + quoteIdentifier(databaseName);
            stmt.execute(createDbSQL);
            LOG.info("Created database if not exists: {}", databaseName);

            // Create table
            String createTableSQL =
                    generateCreateTableDDL(
                            createTableEvent.tableId(),
                            createTableEvent.getSchema(),
                            tableCreateConfig);
            stmt.execute(createTableSQL);
            LOG.info("Successful to create table, event: {}", createTableEvent);
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Failed to create table, event: {}", createTableEvent.tableId(), e);
            throw new SchemaEvolveException(createTableEvent, "Failed to create table", e);
        }
    }

    private void applyAddColumn(AddColumnEvent addColumnEvent) throws SchemaEvolveException {
        TableId tableId = addColumnEvent.tableId();
        try {
            ensureConnection();
            Statement stmt = connection.createStatement();

            for (AddColumnEvent.ColumnWithPosition columnWithPosition :
                    addColumnEvent.getAddedColumns()) {
                Column column = columnWithPosition.getAddColumn();
                String alterSQL =
                        "ALTER TABLE "
                                + quoteIdentifier(tableId.getSchemaName())
                                + "."
                                + quoteIdentifier(tableId.getTableName())
                                + " ADD COLUMN "
                                + quoteIdentifier(column.getName())
                                + " "
                                + ClickHouseUtils.toClickHouseDataType(column)
                                + (column.getType().isNullable() ? "" : " NOT NULL");
                stmt.execute(alterSQL);
            }
            LOG.info("Successful to apply add column, event: {}", addColumnEvent);
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Failed to apply add column, event: {}", addColumnEvent, e);
            throw new SchemaEvolveException(addColumnEvent, "Failed to apply add column", e);
        }
    }

    private void applyDropColumn(DropColumnEvent dropColumnEvent) throws SchemaEvolveException {
        TableId tableId = dropColumnEvent.tableId();
        try {
            ensureConnection();
            Statement stmt = connection.createStatement();

            for (String columnName : dropColumnEvent.getDroppedColumnNames()) {
                String alterSQL =
                        "ALTER TABLE "
                                + quoteIdentifier(tableId.getSchemaName())
                                + "."
                                + quoteIdentifier(tableId.getTableName())
                                + " DROP COLUMN "
                                + quoteIdentifier(columnName);
                stmt.execute(alterSQL);
            }
            LOG.info("Successful to apply drop column, event: {}", dropColumnEvent);
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Failed to apply drop column, event: {}", dropColumnEvent, e);
            throw new SchemaEvolveException(dropColumnEvent, "Failed to apply drop column", e);
        }
    }

    private void applyRenameColumn(RenameColumnEvent renameColumnEvent)
            throws SchemaEvolveException {
        // ClickHouse supports RENAME COLUMN
        TableId tableId = renameColumnEvent.tableId();
        try {
            ensureConnection();
            Statement stmt = connection.createStatement();

            // RenameColumnEvent contains a map of old name -> new name
            for (Map.Entry<String, String> entry : renameColumnEvent.getNameMapping().entrySet()) {
                String alterSQL =
                        "ALTER TABLE "
                                + quoteIdentifier(tableId.getSchemaName())
                                + "."
                                + quoteIdentifier(tableId.getTableName())
                                + " RENAME COLUMN "
                                + quoteIdentifier(entry.getKey())
                                + " TO "
                                + quoteIdentifier(entry.getValue());
                stmt.execute(alterSQL);
            }
            LOG.info("Successful to apply rename column, event: {}", renameColumnEvent);
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Failed to apply rename column, event: {}", renameColumnEvent, e);
            throw new SchemaEvolveException(renameColumnEvent, "Failed to apply rename column", e);
        }
    }

    private void applyAlterColumnType(AlterColumnTypeEvent alterColumnTypeEvent)
            throws SchemaEvolveException {
        // ClickHouse supports MODIFY COLUMN
        TableId tableId = alterColumnTypeEvent.tableId();
        try {
            ensureConnection();
            Statement stmt = connection.createStatement();

            // AlterColumnTypeEvent contains a map of column name -> new DataType
            for (Map.Entry<String, org.apache.flink.cdc.common.types.DataType> entry :
                    alterColumnTypeEvent.getTypeMapping().entrySet()) {
                String columnName = entry.getKey();
                org.apache.flink.cdc.common.types.DataType newType = entry.getValue();
                String alterSQL =
                        "ALTER TABLE "
                                + quoteIdentifier(tableId.getSchemaName())
                                + "."
                                + quoteIdentifier(tableId.getTableName())
                                + " MODIFY COLUMN "
                                + quoteIdentifier(columnName)
                                + " "
                                + ClickHouseUtils.toClickHouseDataType(
                                        Column.physicalColumn(columnName, newType))
                                + (newType.isNullable() ? "" : " NOT NULL");
                stmt.execute(alterSQL);
            }
            LOG.info("Successful to apply alter column type, event: {}", alterColumnTypeEvent);
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Failed to apply alter column type, event: {}", alterColumnTypeEvent, e);
            throw new SchemaEvolveException(
                    alterColumnTypeEvent, "Failed to apply alter column type", e);
        }
    }

    private void applyTruncateTable(TruncateTableEvent truncateTableEvent) {
        TableId tableId = truncateTableEvent.tableId();
        try {
            ensureConnection();
            Statement stmt = connection.createStatement();

            String truncateSQL =
                    "TRUNCATE TABLE "
                            + quoteIdentifier(tableId.getSchemaName())
                            + "."
                            + quoteIdentifier(tableId.getTableName());
            stmt.execute(truncateSQL);
            LOG.info("Successful to apply truncate table, event: {}", truncateTableEvent);
            stmt.close();
        } catch (SQLException e) {
            throw new SchemaEvolveException(truncateTableEvent, e.getMessage(), e);
        }
    }

    private void applyDropTable(DropTableEvent dropTableEvent) {
        TableId tableId = dropTableEvent.tableId();
        try {
            ensureConnection();
            Statement stmt = connection.createStatement();

            String dropSQL =
                    "DROP TABLE IF EXISTS "
                            + quoteIdentifier(tableId.getSchemaName())
                            + "."
                            + quoteIdentifier(tableId.getTableName());
            stmt.execute(dropSQL);
            LOG.info("Successful to apply drop table, event: {}", dropTableEvent);
            stmt.close();
        } catch (SQLException e) {
            throw new SchemaEvolveException(dropTableEvent, e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}

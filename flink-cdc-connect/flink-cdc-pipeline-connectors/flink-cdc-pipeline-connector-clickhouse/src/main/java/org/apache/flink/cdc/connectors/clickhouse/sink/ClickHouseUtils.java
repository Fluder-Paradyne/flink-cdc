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
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeDefaultVisitor;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarCharType;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.ENGINE_MERGE_TREE;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.ENGINE_REPLACING_MERGE_TREE_NEW;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.IS_DELETED_COLUMN;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.IS_DELETED_COLUMN_DATA_TYPE;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.VERSION_COLUMN;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseConstants.VERSION_COLUMN_DATA_TYPE;

/** Utilities for conversion from source table to ClickHouse table. */
public class ClickHouseUtils {

    /** Format DATE type data. */
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Format timestamp-related type data. */
    private static final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Creates an accessor for getting elements in an internal RecordData structure at the given
     * position.
     *
     * @param fieldType the element type of the RecordData
     * @param fieldPos the element position of the RecordData
     * @param zoneId the time zone used when converting from <code>TIMESTAMP WITH LOCAL TIME ZONE
     *     </code>
     */
    public static RecordData.FieldGetter createFieldGetter(
            DataType fieldType, int fieldPos, ZoneId zoneId) {
        final RecordData.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                fieldGetter = record -> record.getBoolean(fieldPos);
                break;
            case TINYINT:
                fieldGetter = record -> record.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = record -> record.getShort(fieldPos);
                break;
            case INTEGER:
                fieldGetter = record -> record.getInt(fieldPos);
                break;
            case BIGINT:
                fieldGetter = record -> record.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = record -> record.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = record -> record.getDouble(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter =
                        record ->
                                record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                        .toBigDecimal();
                break;
            case CHAR:
            case VARCHAR:
                fieldGetter = record -> record.getString(fieldPos).toString();
                break;
            case DATE:
                fieldGetter =
                        record -> record.getDate(fieldPos).toLocalDate().format(DATE_FORMATTER);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        record ->
                                record.getTimestamp(fieldPos, getPrecision(fieldType))
                                        .toLocalDateTime()
                                        .format(DATETIME_FORMATTER);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        record ->
                                ZonedDateTime.ofInstant(
                                                record.getLocalZonedTimestampData(
                                                                fieldPos, getPrecision(fieldType))
                                                        .toInstant(),
                                                zoneId)
                                        .toLocalDateTime()
                                        .format(DATETIME_FORMATTER);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Don't support data type " + fieldType.getTypeRoot());
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /**
     * Convert CDC data type to ClickHouse data type string.
     *
     * @param cdcColumn the CDC column
     * @return ClickHouse data type string
     */
    public static String toClickHouseDataType(Column cdcColumn) {
        CdcDataTypeTransformer dataTypeTransformer = new CdcDataTypeTransformer();
        return cdcColumn.getType().accept(dataTypeTransformer);
    }

    /**
     * Generate CREATE TABLE DDL for ClickHouse with CDC system columns.
     *
     * <p>For tables with primary keys, creates a ReplacingMergeTree with:
     *
     * <ul>
     *   <li>{@code _version} - UInt64 for version-based deduplication
     *   <li>{@code _is_deleted} - UInt8 flag for soft deletes (0=active, 1=deleted)
     * </ul>
     *
     * <p>This approach allows:
     *
     * <ul>
     *   <li>INSERT: Insert with _version, _is_deleted=0
     *   <li>UPDATE: Insert new row with higher _version, _is_deleted=0
     *   <li>DELETE: Insert row with higher _version, _is_deleted=1
     * </ul>
     *
     * @param schema the table schema
     * @param tableCreateConfig table creation configuration
     * @return CREATE TABLE DDL statement
     */
    public static String generateCreateTableDDL(
            org.apache.flink.cdc.common.event.TableId tableId,
            Schema schema,
            TableCreateConfig tableCreateConfig) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ");
        ddl.append(quoteIdentifier(tableId.getSchemaName()));
        ddl.append(".");
        ddl.append(quoteIdentifier(tableId.getTableName()));
        ddl.append(" (");

        List<String> columnDefinitions = new ArrayList<>();

        // Add user-defined columns
        for (Column column : schema.getColumns()) {
            String columnDef =
                    quoteIdentifier(column.getName())
                            + " "
                            + toClickHouseDataType(column)
                            + (column.getType().isNullable() ? "" : " NOT NULL");
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                columnDef += " COMMENT '" + escapeString(column.getComment()) + "'";
            }
            columnDefinitions.add(columnDef);
        }

        // Add CDC system columns for tables with primary keys
        if (!schema.primaryKeys().isEmpty()) {
            // _version column for ReplacingMergeTree ordering
            columnDefinitions.add(
                    quoteIdentifier(VERSION_COLUMN)
                            + " "
                            + VERSION_COLUMN_DATA_TYPE
                            + " COMMENT 'CDC version for deduplication'");

            // _is_deleted column for soft deletes
            columnDefinitions.add(
                    quoteIdentifier(IS_DELETED_COLUMN)
                            + " "
                            + IS_DELETED_COLUMN_DATA_TYPE
                            + " DEFAULT 0"
                            + " COMMENT 'CDC delete flag (0=active, 1=deleted)'");
        }

        ddl.append(String.join(", ", columnDefinitions));
        ddl.append(")");

        // Choose engine based on primary keys
        if (!schema.primaryKeys().isEmpty()) {
            // ReplacingMergeTree with version and is_deleted for CDC
            ddl.append(" ENGINE = ").append(ENGINE_REPLACING_MERGE_TREE_NEW);
        } else {
            // Simple MergeTree for tables without primary keys
            ddl.append(" ENGINE = ").append(ENGINE_MERGE_TREE);
        }

        // Add ORDER BY clause (required for MergeTree engines, comes AFTER ENGINE)
        if (!schema.primaryKeys().isEmpty()) {
            ddl.append(" ORDER BY (");
            List<String> pkColumns = new ArrayList<>();
            for (String pk : schema.primaryKeys()) {
                pkColumns.add(quoteIdentifier(pk));
            }
            ddl.append(String.join(", ", pkColumns));
            ddl.append(")");
        } else {
            // If no primary key, use first column as ORDER BY
            if (!schema.getColumns().isEmpty()) {
                ddl.append(" ORDER BY (");
                ddl.append(quoteIdentifier(schema.getColumns().get(0).getName()));
                ddl.append(")");
            }
        }

        // Add table properties if any
        if (!tableCreateConfig.getProperties().isEmpty()) {
            // ClickHouse table properties can be added here if needed
        }

        return ddl.toString();
    }

    /**
     * Get the list of column names including CDC system columns.
     *
     * @param schema the table schema
     * @param includeSystemColumns whether to include _version and _is_deleted columns
     * @return list of column names
     */
    public static List<String> getColumnNames(Schema schema, boolean includeSystemColumns) {
        List<String> columnNames = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            columnNames.add(column.getName());
        }
        if (includeSystemColumns && !schema.primaryKeys().isEmpty()) {
            columnNames.add(VERSION_COLUMN);
            columnNames.add(IS_DELETED_COLUMN);
        }
        return columnNames;
    }

    /**
     * Build WHERE clause for DELETE/UPDATE operations based on primary keys.
     *
     * @param schema the table schema
     * @param record the record data
     * @param zoneId the time zone for timestamp conversion
     * @return WHERE clause string
     */
    public static String buildPrimaryKeyWhereClause(
            Schema schema, RecordData record, ZoneId zoneId) {
        if (schema.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "DELETE/UPDATE operations require primary keys, but table has no primary keys");
        }

        List<String> conditions = new ArrayList<>();
        RecordData.FieldGetter[] fieldGetters = new RecordData.FieldGetter[schema.getColumnCount()];
        for (int i = 0; i < schema.getColumnCount(); i++) {
            fieldGetters[i] = createFieldGetter(schema.getColumns().get(i).getType(), i, zoneId);
        }

        for (String pk : schema.primaryKeys()) {
            int pkIndex = -1;
            for (int i = 0; i < schema.getColumnCount(); i++) {
                if (schema.getColumns().get(i).getName().equals(pk)) {
                    pkIndex = i;
                    break;
                }
            }
            if (pkIndex == -1) {
                throw new RuntimeException("Primary key column " + pk + " not found in schema");
            }

            Object value = fieldGetters[pkIndex].getFieldOrNull(record);
            if (value == null) {
                conditions.add(quoteIdentifier(pk) + " IS NULL");
            } else {
                conditions.add(quoteIdentifier(pk) + " = " + formatValueForSQL(value));
            }
        }

        return String.join(" AND ", conditions);
    }

    /**
     * Format a value for use in SQL WHERE clause.
     *
     * @param value the value to format
     * @return formatted SQL value string
     */
    private static String formatValueForSQL(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + escapeString((String) value) + "'";
        } else if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        } else {
            return "'" + escapeString(value.toString()) + "'";
        }
    }

    /** Quote identifier for ClickHouse. */
    public static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    /** Escape string for ClickHouse. */
    public static String escapeString(String str) {
        return str.replace("'", "''").replace("\\", "\\\\");
    }

    // ------------------------------------------------------------------------------------------
    // ClickHouse data types
    // ------------------------------------------------------------------------------------------

    public static final String BOOLEAN = "Bool";
    public static final String TINYINT = "Int8";
    public static final String SMALLINT = "Int16";
    public static final String INT = "Int32";
    public static final String BIGINT = "Int64";
    public static final String FLOAT = "Float32";
    public static final String DOUBLE = "Float64";
    public static final String DECIMAL = "Decimal";
    public static final String STRING = "String";
    public static final String FIXEDSTRING = "FixedString";
    public static final String DATE = "Date";
    public static final String DATETIME = "DateTime";
    public static final String DATETIME64 = "DateTime64";

    /** Transforms CDC {@link DataType} to ClickHouse data type. */
    public static class CdcDataTypeTransformer extends DataTypeDefaultVisitor<String> {

        @Override
        public String visit(BooleanType booleanType) {
            return BOOLEAN;
        }

        @Override
        public String visit(TinyIntType tinyIntType) {
            return TINYINT;
        }

        @Override
        public String visit(SmallIntType smallIntType) {
            return SMALLINT;
        }

        @Override
        public String visit(IntType intType) {
            return INT;
        }

        @Override
        public String visit(BigIntType bigIntType) {
            return BIGINT;
        }

        @Override
        public String visit(FloatType floatType) {
            return FLOAT;
        }

        @Override
        public String visit(DoubleType doubleType) {
            return DOUBLE;
        }

        @Override
        public String visit(DecimalType decimalType) {
            return DECIMAL + "(" + decimalType.getPrecision() + "," + decimalType.getScale() + ")";
        }

        @Override
        public String visit(CharType charType) {
            int length = charType.getLength();
            if (length <= 255) {
                return FIXEDSTRING + "(" + length + ")";
            } else {
                return STRING;
            }
        }

        @Override
        public String visit(VarCharType varCharType) {
            return STRING;
        }

        @Override
        public String visit(DateType dateType) {
            return DATE;
        }

        @Override
        public String visit(TimestampType timestampType) {
            int precision = getPrecision(timestampType);
            if (precision <= 3) {
                return DATETIME;
            } else {
                return DATETIME64 + "(" + precision + ")";
            }
        }

        @Override
        public String visit(LocalZonedTimestampType localZonedTimestampType) {
            int precision = getPrecision(localZonedTimestampType);
            if (precision <= 3) {
                return DATETIME;
            } else {
                return DATETIME64 + "(" + precision + ")";
            }
        }

        @Override
        protected String defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException("Unsupported CDC data type " + dataType);
        }
    }
}

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
import org.apache.flink.cdc.common.event.TableId;
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
     * Generate CREATE TABLE DDL for ClickHouse.
     *
     * @param tableId the table identifier
     * @param schema the table schema
     * @param tableCreateConfig table creation configuration
     * @return CREATE TABLE DDL statement
     */
    public static String generateCreateTableDDL(
            TableId tableId, Schema schema, TableCreateConfig tableCreateConfig) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ");
        ddl.append(quoteIdentifier(tableId.getSchemaName()));
        ddl.append(".");
        ddl.append(quoteIdentifier(tableId.getTableName()));
        ddl.append(" (");

        List<String> columnDefinitions = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            String columnDef =
                    quoteIdentifier(column.getName())
                            + " "
                            + toClickHouseDataType(column)
                            + (column.isNullable() ? "" : " NOT NULL");
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                columnDef += " COMMENT '" + escapeString(column.getComment()) + "'";
            }
            columnDefinitions.add(columnDef);
        }

        ddl.append(String.join(", ", columnDefinitions));

        // Add primary key if exists (ClickHouse supports ORDER BY instead of PRIMARY KEY for MergeTree)
        if (!schema.primaryKeys().isEmpty()) {
            ddl.append(", ORDER BY (");
            List<String> pkColumns = new ArrayList<>();
            for (String pk : schema.primaryKeys()) {
                pkColumns.add(quoteIdentifier(pk));
            }
            ddl.append(String.join(", ", pkColumns));
            ddl.append(")");
        } else {
            // If no primary key, use first column as ORDER BY
            if (!schema.getColumns().isEmpty()) {
                ddl.append(", ORDER BY (");
                ddl.append(quoteIdentifier(schema.getColumns().get(0).getName()));
                ddl.append(")");
            }
        }

        ddl.append(") ENGINE = MergeTree()");

        // Add table properties if any
        if (!tableCreateConfig.getProperties().isEmpty()) {
            // ClickHouse table properties can be added here if needed
        }

        return ddl.toString();
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
    public static class CdcDataTypeTransformer
            extends DataTypeDefaultVisitor<String> {

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
            return DECIMAL
                    + "("
                    + decimalType.getPrecision()
                    + ","
                    + decimalType.getScale()
                    + ")";
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


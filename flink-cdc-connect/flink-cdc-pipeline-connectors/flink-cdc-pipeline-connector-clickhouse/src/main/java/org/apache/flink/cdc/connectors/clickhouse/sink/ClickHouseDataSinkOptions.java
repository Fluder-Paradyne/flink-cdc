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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import java.time.Duration;

/** Options for {@link ClickHouseDataSink}. */
public class ClickHouseDataSinkOptions {

    // ------------------------------------------------------------------------------------------
    // Options for sink connector
    // ------------------------------------------------------------------------------------------

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "ClickHouse server URL, e.g., 'http://localhost:8123' or 'clickhouse://localhost:9000'.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse user name.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse user password.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse database name.");

    public static final ConfigOption<Integer> SINK_BATCH_SIZE =
            ConfigOptions.key("sink.batch-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Batch size for writing data to ClickHouse.");

    public static final ConfigOption<Long> SINK_FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush-interval-ms")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription(
                            "Flush interval in milliseconds for writing data to ClickHouse.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum number of retries for failed writes.");

    public static final ConfigOption<Integer> SINK_CONNECTION_TIMEOUT =
            ConfigOptions.key("sink.connection-timeout-ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Connection timeout in milliseconds.");

    public static final ConfigOption<Integer> SINK_SOCKET_TIMEOUT =
            ConfigOptions.key("sink.socket-timeout-ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Socket timeout in milliseconds.");

    // ------------------------------------------------------------------------------------------
    // Options for schema change
    // ------------------------------------------------------------------------------------------

    /**
     * The prefix for properties used for creating a table. You can refer to ClickHouse
     * documentation for the DDL.
     */
    public static final String TABLE_CREATE_PROPERTIES_PREFIX = "table.create.properties.";

    public static final ConfigOption<Duration> TABLE_SCHEMA_CHANGE_TIMEOUT =
            ConfigOptions.key("table.schema-change.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1800))
                    .withDescription(
                            "Timeout for a schema change on ClickHouse side, and must be an integral multiple of "
                                    + "seconds. ClickHouse will cancel the schema change after timeout which will "
                                    + "cause the sink failure.");
}

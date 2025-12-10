package org.apache.flink.cdc.connectors.clickhouse.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import java.util.HashSet;
import java.util.Set;

public class ClickHouseDataSinkOptions {
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

    public static Set<ConfigOption<?>> getRequiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        return options;
    }
}

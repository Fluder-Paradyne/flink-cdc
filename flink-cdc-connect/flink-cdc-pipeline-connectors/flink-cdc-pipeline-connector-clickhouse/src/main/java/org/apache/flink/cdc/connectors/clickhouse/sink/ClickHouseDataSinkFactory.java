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
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;
import static org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseDataSinkOptions.TABLE_CREATE_PROPERTIES_PREFIX;

/** A {@link DataSinkFactory} to create {@link ClickHouseDataSink}. */
public class ClickHouseDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "clickhouse";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(TABLE_CREATE_PROPERTIES_PREFIX);

        ClickHouseDataSink.ClickHouseSinkConfig sinkConfig =
                buildSinkConfig(context.getFactoryConfiguration());
        TableCreateConfig tableCreateConfig =
                TableCreateConfig.from(context.getFactoryConfiguration());
        SchemaChangeConfig schemaChangeConfig =
                SchemaChangeConfig.from(context.getFactoryConfiguration());
        String zoneStr = context.getPipelineConfiguration().get(PIPELINE_LOCAL_TIME_ZONE);
        ZoneId zoneId =
                PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(zoneStr)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zoneStr);
        return new ClickHouseDataSink(sinkConfig, tableCreateConfig, schemaChangeConfig, zoneId);
    }

    private ClickHouseDataSink.ClickHouseSinkConfig buildSinkConfig(Configuration cdcConfig) {
        String url = cdcConfig.get(ClickHouseDataSinkOptions.URL);
        String username = cdcConfig.get(ClickHouseDataSinkOptions.USERNAME);
        String password = cdcConfig.get(ClickHouseDataSinkOptions.PASSWORD);
        String databaseName = cdcConfig.get(ClickHouseDataSinkOptions.DATABASE_NAME);
        int batchSize = cdcConfig.get(ClickHouseDataSinkOptions.SINK_BATCH_SIZE);
        long flushInterval = cdcConfig.get(ClickHouseDataSinkOptions.SINK_FLUSH_INTERVAL);

        return new ClickHouseDataSink.ClickHouseSinkConfig(
                url, username, password, databaseName, batchSize, flushInterval);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(ClickHouseDataSinkOptions.URL);
        requiredOptions.add(ClickHouseDataSinkOptions.USERNAME);
        requiredOptions.add(ClickHouseDataSinkOptions.PASSWORD);
        requiredOptions.add(ClickHouseDataSinkOptions.DATABASE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(ClickHouseDataSinkOptions.SINK_BATCH_SIZE);
        optionalOptions.add(ClickHouseDataSinkOptions.SINK_FLUSH_INTERVAL);
        optionalOptions.add(ClickHouseDataSinkOptions.SINK_MAX_RETRIES);
        optionalOptions.add(ClickHouseDataSinkOptions.SINK_CONNECTION_TIMEOUT);
        optionalOptions.add(ClickHouseDataSinkOptions.SINK_SOCKET_TIMEOUT);
        optionalOptions.add(ClickHouseDataSinkOptions.TABLE_SCHEMA_CHANGE_TIMEOUT);
        return optionalOptions;
    }
}

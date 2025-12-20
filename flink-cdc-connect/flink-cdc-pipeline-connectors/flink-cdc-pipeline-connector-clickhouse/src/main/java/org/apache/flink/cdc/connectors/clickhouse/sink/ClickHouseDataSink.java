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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.io.Serializable;
import java.time.ZoneId;

/** A {@link DataSink} for ClickHouse connector that supports schema evolution. */
public class ClickHouseDataSink implements DataSink, Serializable {

    private static final long serialVersionUID = 1L;

    /** Configurations for sink connector. */
    private final ClickHouseSinkConfig sinkConfig;

    /** Configurations for creating a ClickHouse table. */
    private final TableCreateConfig tableCreateConfig;

    /** Configurations for schema change. */
    private final SchemaChangeConfig schemaChangeConfig;

    /**
     * The local time zone used when converting from <code>TIMESTAMP WITH LOCAL TIME ZONE</code>.
     */
    private final ZoneId zoneId;

    public ClickHouseDataSink(
            ClickHouseSinkConfig sinkConfig,
            TableCreateConfig tableCreateConfig,
            SchemaChangeConfig schemaChangeConfig,
            ZoneId zoneId) {
        this.sinkConfig = sinkConfig;
        this.tableCreateConfig = tableCreateConfig;
        this.schemaChangeConfig = schemaChangeConfig;
        this.zoneId = zoneId;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        ClickHouseDataSinkFunction sinkFunction =
                new ClickHouseDataSinkFunction(
                        sinkConfig.getUrl(),
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword(),
                        sinkConfig.getDatabaseName(),
                        sinkConfig.getBatchSize(),
                        sinkConfig.getFlushInterval(),
                        zoneId);
        return FlinkSinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new ClickHouseMetadataApplier(
                sinkConfig.getUrl(),
                sinkConfig.getUsername(),
                sinkConfig.getPassword(),
                sinkConfig.getDatabaseName(),
                tableCreateConfig,
                schemaChangeConfig);
    }

    @VisibleForTesting
    public ZoneId getZoneId() {
        return zoneId;
    }

    /** Sink configuration holder. */
    public static class ClickHouseSinkConfig implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String url;
        private final String username;
        private final String password;
        private final String databaseName;
        private final int batchSize;
        private final long flushInterval;

        public ClickHouseSinkConfig(
                String url,
                String username,
                String password,
                String databaseName,
                int batchSize,
                long flushInterval) {
            this.url = url;
            this.username = username;
            this.password = password;
            this.databaseName = databaseName;
            this.batchSize = batchSize;
            this.flushInterval = flushInterval;
        }

        public String getUrl() {
            return url;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public long getFlushInterval() {
            return flushInterval;
        }
    }
}

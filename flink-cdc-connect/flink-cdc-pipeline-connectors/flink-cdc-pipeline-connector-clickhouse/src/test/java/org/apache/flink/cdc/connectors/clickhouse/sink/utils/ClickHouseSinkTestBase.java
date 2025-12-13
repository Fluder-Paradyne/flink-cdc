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

package org.apache.flink.cdc.connectors.clickhouse.sink.utils;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseDataSink;
import org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseDataSinkFactory;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/** Basic class for testing {@link ClickHouseDataSink}. */
public class ClickHouseSinkTestBase extends TestLogger {
    protected static final Logger LOG = LoggerFactory.getLogger(ClickHouseSinkTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 1;

    protected static final ClickHouseContainer CLICKHOUSE_CONTAINER = new ClickHouseContainer();

    public static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 120;

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting ClickHouse container...");
        Startables.deepStart(Stream.of(CLICKHOUSE_CONTAINER)).join();
        LOG.info("ClickHouse container started at {}", CLICKHOUSE_CONTAINER.getHttpUrl());
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        CLICKHOUSE_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    static class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return Configuration.fromMap(Collections.singletonMap("local-time-zone", "UTC"));
        }

        @Override
        public ClassLoader getClassLoader() {
            return null;
        }
    }

    public static DataSink createClickHouseDataSink(Configuration factoryConfiguration) {
        ClickHouseDataSinkFactory factory = new ClickHouseDataSinkFactory();
        return factory.createDataSink(new MockContext(factoryConfiguration));
    }

    public static void executeSql(String sql) {
        try {
            String jdbcUrl = CLICKHOUSE_CONTAINER.getJdbcUrl();
            Properties properties = new Properties();
            properties.setProperty("user", CLICKHOUSE_CONTAINER.getUsername());
            properties.setProperty("password", CLICKHOUSE_CONTAINER.getPassword());

            try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
                    Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    public List<String> inspectTableSchema(TableId tableId) throws SQLException {
        List<String> results = new ArrayList<>();
        String jdbcUrl = CLICKHOUSE_CONTAINER.getJdbcUrl();
        Properties properties = new Properties();
        properties.setProperty("user", CLICKHOUSE_CONTAINER.getUsername());
        properties.setProperty("password", CLICKHOUSE_CONTAINER.getPassword());

        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
                Statement stmt = connection.createStatement()) {
            ResultSet rs =
                    stmt.executeQuery(
                            String.format(
                                    "DESCRIBE TABLE `%s`.`%s`",
                                    tableId.getSchemaName(), tableId.getTableName()));

            while (rs.next()) {
                List<String> columns = new ArrayList<>();
                int columnCount = rs.getMetaData().getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    columns.add(rs.getString(i));
                }
                results.add(String.join(" | ", columns));
            }
        }
        return results;
    }

    public List<String> fetchTableContent(TableId tableId, int columnCount) throws SQLException {
        List<String> results = new ArrayList<>();
        String jdbcUrl = CLICKHOUSE_CONTAINER.getJdbcUrl();
        Properties properties = new Properties();
        properties.setProperty("user", CLICKHOUSE_CONTAINER.getUsername());
        properties.setProperty("password", CLICKHOUSE_CONTAINER.getPassword());

        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
                Statement stmt = connection.createStatement()) {
            ResultSet rs =
                    stmt.executeQuery(
                            String.format(
                                    "SELECT * FROM `%s`.`%s`",
                                    tableId.getSchemaName(), tableId.getTableName()));

            while (rs.next()) {
                List<String> columns = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    columns.add(rs.getString(i));
                }
                results.add(String.join(" | ", columns));
            }
        }
        return results;
    }

    public static <T> void assertEqualsInAnyOrder(List<T> expected, List<T> actual) {
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    public static <T> void assertEqualsInOrder(List<T> expected, List<T> actual) {
        Assertions.assertThat(actual).containsExactlyElementsOf(expected);
    }

    public static <K, V> void assertMapEquals(Map<K, V> expected, Map<K, V> actual) {
        Assertions.assertThat(actual).containsExactlyEntriesOf(expected);
    }
}

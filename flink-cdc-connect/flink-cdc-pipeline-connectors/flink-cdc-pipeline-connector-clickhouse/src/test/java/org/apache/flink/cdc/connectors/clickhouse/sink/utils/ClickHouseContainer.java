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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/** Docker container for ClickHouse. */
public class ClickHouseContainer extends GenericContainer<ClickHouseContainer> {

    private static final String DOCKER_IMAGE_NAME = "clickhouse/clickhouse-server:latest";

    // exposed ports
    public static final int HTTP_PORT = 8123;
    public static final int NATIVE_PORT = 9000;

    public static final String CLICKHOUSE_DATABASE_NAME = "test_db";
    public static final String CLICKHOUSE_TABLE_NAME = "test_table";
    public static final String CLICKHOUSE_USERNAME = "default";
    public static final String CLICKHOUSE_PASSWORD = "";

    public ClickHouseContainer() {
        super(DockerImageName.parse(DOCKER_IMAGE_NAME));
        withExposedPorts(HTTP_PORT, NATIVE_PORT);
        waitingFor(
                new HttpWaitStrategy()
                        .forPath("/ping")
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    public String getHttpUrl() {
        return "http://" + getHost() + ":" + getMappedPort(HTTP_PORT);
    }

    public String getJdbcUrl() {
        return "jdbc:clickhouse://" + getHost() + ":" + getMappedPort(HTTP_PORT);
    }

    public String getJdbcUrl(String databaseName) {
        return getJdbcUrl() + "/" + databaseName;
    }

    public String getUsername() {
        return CLICKHOUSE_USERNAME;
    }

    public String getPassword() {
        return CLICKHOUSE_PASSWORD;
    }
}

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

import org.apache.flink.cdc.common.configuration.Configuration;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Configurations for creating a ClickHouse table. See <a
 * href="https://clickhouse.com/docs/en/sql-reference/statements/create/table">ClickHouse Documentation</a>
 * for how to create a ClickHouse table.
 */
public class TableCreateConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Properties for the table. */
    private final Map<String, String> properties;

    public TableCreateConfig(Map<String, String> properties) {
        this.properties = new HashMap<>(properties);
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public static TableCreateConfig from(Configuration config) {
        Map<String, String> tableProperties =
                config.toMap().entrySet().stream()
                        .filter(
                                entry ->
                                        entry.getKey()
                                                .startsWith(
                                                        ClickHouseDataSinkOptions
                                                                .TABLE_CREATE_PROPERTIES_PREFIX))
                        .collect(
                                Collectors.toMap(
                                        entry ->
                                                entry.getKey()
                                                        .substring(
                                                                ClickHouseDataSinkOptions
                                                                        .TABLE_CREATE_PROPERTIES_PREFIX
                                                                        .length())
                                                        .toLowerCase(),
                                        Map.Entry::getValue));
        return new TableCreateConfig(tableProperties);
    }
}


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

package org.apache.flink.cdc.connectors.clickhouse.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.clickhouse.sink.ClickHouseDataSinkOptions;

import java.util.HashSet;
import java.util.Set;

/** A {@link DataSinkFactory}. */
@Internal
public class ClickHouseDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "clickhouse";

    /**
     * Creates a {@link DataSink} instance.
     *
     * @param context
     */
    @Override
    public DataSink createDataSink(Context context) {
        Configuration config = context.getFactoryConfiguration();

        return null;
    }

    /** Returns a unique identifier among same factory interfaces. */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory requires in
     * addition to {@link #optionalOptions()}.
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.addAll(ClickHouseDataSinkOptions.getRequiredOptions());
        return options;
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory consumes in
     * addition to {@link #requiredOptions()}.
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }
}

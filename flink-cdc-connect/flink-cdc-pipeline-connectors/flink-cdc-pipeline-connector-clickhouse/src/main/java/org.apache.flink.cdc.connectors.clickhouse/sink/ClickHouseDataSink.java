package org.apache.flink.cdc.connectors.clickhouse.sink;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.io.Serializable;

public class ClickHouseDataSink implements DataSink, Serializable {

    /** Get the {@link EventSinkProvider} for writing changed data to external systems. */
    @Override
    public EventSinkProvider getEventSinkProvider() {
        return null;
    }

    /** Get the {@link MetadataApplier} for applying metadata changes to external systems. */
    @Override
    public MetadataApplier getMetadataApplier() {
        return null;
    }

    /**
     * Get the {@code HashFunctionProvider<DataChangeEvent>} for calculating hash value if you need
     * to partition by data change event before Sink.
     */
    @Override
    public HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider() {
        return DataSink.super.getDataChangeEventHashFunctionProvider();
    }
}

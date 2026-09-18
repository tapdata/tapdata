package io.tapdata.flow.engine.V2.node.hazelcast.dynamic.proxy;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.schema.TapTableMap;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamReadFunctionProxyTest {

    @Test
    void rejectsLogCdcWhenSelectedTableIsView() throws Throwable {
        TapConnectorContext context = mock(TapConnectorContext.class);
        TapTable view = new TapTable("orders_view");
        view.setType("view");
        KVReadOnlyMap<TapTable> tableMap = mock(KVReadOnlyMap.class);
        Iterator<Entry<TapTable>> iterator = mock(Iterator.class);
        Entry<TapTable> entry = mock(Entry.class);
        when(tableMap.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(entry);
        when(entry.getKey()).thenReturn("orders_view");
        when(entry.getValue()).thenReturn(view);
        when(context.getTableMap()).thenReturn(tableMap);
        when(context.getLog()).thenReturn(mock(Log.class));

        StreamReadFunctionProxy proxy = StreamReadFunctionProxy.instance(mock(StreamReadFunction.class));
        StreamReadConsumer consumer = mock(StreamReadConsumer.class);
        ArrayList<String> tables = new ArrayList<>(Collections.singletonList("orders_view"));
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> proxy.streamRead(context, tables, null, 100, consumer));

        assertTrue(error.getMessage().contains("View only supports polling incremental mode"));
        assertTrue(error.getMessage().contains("orders_view"));
        assertTrue(tables.contains("orders_view"));
    }

    @Test
    void rejectsViewBeforeRemovingItFromLogCdcTableMap() {
        TapTable view = new TapTable("orders_view");
        view.setType("view");
        TapTableMap<String, TapTable> tableMap = mock(TapTableMap.class);
        Iterator<Entry<TapTable>> iterator = mock(Iterator.class);
        Entry<TapTable> entry = mock(Entry.class);
        when(tableMap.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(entry);
        when(entry.getKey()).thenReturn("orders_view");
        when(entry.getValue()).thenReturn(view);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> StreamReadBaseProxy.judgeTable(tableMap, mock(ObsLogger.class)));

        assertTrue(error.getMessage().contains("orders_view"));
        verify(tableMap, never()).remove("orders_view");
    }
}

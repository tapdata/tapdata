package io.tapdata.flow.engine.V2.node.hazelcast.dynamic.proxy;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManagerSchemaEnforced;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.schema.TapTableMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class StreamReadViewSchemaTest {
    @Test
    @DisplayName("纯视图跳过CDC后，排队的全量数据仍可使用原模型进行强类型转换")
    void snapshotSchemaSurvivesSkippingCdc() {
        TapTable view = table("v_test", "view");
        TapTableMap<String, TapTable> map = map(view);

        assertFalse(StreamReadBaseProxy.judgeTable(map, mock(ObsLogger.class)));
        assertTrue(StreamReadBaseProxy.getCdcTableNames(map).isEmpty());
        assertSame(view, map.get("v_test"));
        assertEquals(1, map.size());
        Map<String, Object> record = new HashMap<>(Map.of("name", "snapshot"));
        TapCodecsFilterManagerSchemaEnforced codecs =
                new TapCodecsFilterManagerSchemaEnforced(new TapCodecsRegistry());
        assertDoesNotThrow(() -> codecs.transformToTapValueMap(record, map.get("v_test")));
    }

    @Test
    @DisplayName("混选仅订阅普通表，保留视图及Mongo视图的全量模型")
    void mixedTablesUseIndependentSubscriptionList() {
        TapTable view = table("v_test", "view");
        TapTable mongoView = table("mv_test", "mongo_view");
        TapTable physical = table("test", "table");
        TapTableMap<String, TapTable> map = map(view, mongoView, physical);

        assertTrue(StreamReadBaseProxy.judgeTable(map, mock(ObsLogger.class)));
        List<String> subscription = StreamReadBaseProxy.getCdcTableNames(map);
        assertEquals(List.of("test"), subscription);
        subscription.clear();
        assertEquals(3, map.size());
        assertSame(view, map.get("v_test"));
        assertSame(mongoView, map.get("mv_test"));
        assertSame(physical, map.get("test"));
    }

    @Test
    @DisplayName("空表列表仍跳过CDC")
    void emptyMapSkipsCdc() {
        assertFalse(StreamReadBaseProxy.judgeTable(map(), mock(ObsLogger.class)));
    }

    private TapTable table(String name, String type) {
        TapTable table = new TapTable(name);
        table.setType(type);
        table.add(new TapField("name", "varchar").tapType(new TapString()));
        return table;
    }

    private TapTableMap<String, TapTable> map(TapTable... tables) {
        Map<String, String> names = new HashMap<>();
        for (TapTable table : tables) names.put(table.getId(), table.getId());
        TapTableMap<String, TapTable> map = new TapTableMap<String, TapTable>("source", 0L, names) {};
        for (TapTable table : tables) map.put(table.getId(), table);
        return map;
    }
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.cache.CacheUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.mockito.Mockito.*;

class HazelcastTargetPdkCacheNodeTest extends HazelcastPdkBaseNodeTest{
    HazelcastTargetPdkCacheNode hazelcastTargetPdkCacheNode;
    ConstructIMap<Map<String, Map<String, Object>>> dataMap;
    @BeforeEach
    void init(){
        hazelcastTargetPdkCacheNode = mock(HazelcastTargetPdkCacheNode.class);
        DataFlowCacheConfig dataFlowCacheConfig = new DataFlowCacheConfig("field1", "cacheTest", "all", 1000L, 500L, 1000L, Set.of("field1", "field2"), null, null, "test", null, List.of("field1"));
        ReflectionTestUtils.setField(hazelcastTargetPdkCacheNode, "dataFlowCacheConfig", dataFlowCacheConfig);
        dataMap = mock(ConstructIMap.class);
        ReflectionTestUtils.setField(hazelcastTargetPdkCacheNode, "dataMap", dataMap);
    }
    @Test
    @SneakyThrows
    void processEventsTest() {
        List<TapEvent> tapEvents = new ArrayList<>();
        TapUpdateRecordEvent tapEvent = new TapUpdateRecordEvent();
        Map<String, Object> before = new HashMap<>();
        before.put("field1", "value1");
        before.put("field2", "value2");
        before.put("field3", "value3");
        before.put("field4", "value4");
        tapEvent.setBefore(before);
        Map<String, Object> after = new HashMap<>();
        after.put("field1", "value1");
        after.put("field2", "value2-test");
        after.put("field3", "value3");
        after.put("field4", "value4");
        tapEvent.setAfter(after);
        tapEvents.add(tapEvent);
        doCallRealMethod().when(hazelcastTargetPdkCacheNode).processEvents(tapEvents);
        hazelcastTargetPdkCacheNode.processEvents(tapEvents);
        Map<String, Object> cacheFieldRow = new HashMap<>();
        cacheFieldRow.put("field1", "value1");
        cacheFieldRow.put("field2", "value2-test");
        Map<String, Map<String, Object>> recordMap = new HashMap<>();
        recordMap.put("value1-", cacheFieldRow);
        verify(dataMap, times(1)).insert("value1-", recordMap);
    }
}

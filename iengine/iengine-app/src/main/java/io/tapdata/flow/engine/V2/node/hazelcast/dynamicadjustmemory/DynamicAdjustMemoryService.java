package io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory;

import io.tapdata.entity.event.TapEvent;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-07-20 11:18
 **/
public interface DynamicAdjustMemoryService {
	DynamicAdjustResult calcQueueSize(List<TapEvent> events, int originalQueueSize);
}

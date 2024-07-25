package io.tapdata.flow.engine.V2.common;

import com.tapdata.entity.TapdataEvent;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-07-06 17:25
 **/
public interface TapdataEventsRunner {
	void run(List<TapdataEvent> tapdataEventList);
}

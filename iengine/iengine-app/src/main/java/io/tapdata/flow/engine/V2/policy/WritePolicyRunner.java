package io.tapdata.flow.engine.V2.policy;

import io.tapdata.entity.event.dml.TapRecordEvent;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-07-09 15:51
 **/
public interface WritePolicyRunner {
	void run(List<TapRecordEvent> tapRecordEvents);
}

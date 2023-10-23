package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.util.TapEventUtil;

import java.util.function.BiConsumer;

/**
 * @author samuel
 * @Description
 * @create 2022-03-04 23:34
 **/
public class HazelcastBlank extends HazelcastProcessorBaseNode {

	public HazelcastBlank(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		String tableName = TapEventUtil.getTableId(tapEvent);
		ProcessResult processResult = getProcessResult(tableName);
		consumer.accept(tapdataEvent, processResult);
	}
}

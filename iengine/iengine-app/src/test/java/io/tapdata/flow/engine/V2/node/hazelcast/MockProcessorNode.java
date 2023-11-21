package io.tapdata.flow.engine.V2.node.hazelcast;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;

import java.util.function.BiConsumer;

/**
 * @author samuel
 * @Description
 * @create 2023-11-17 18:40
 **/
public class MockProcessorNode extends HazelcastProcessorBaseNode {
	public MockProcessorNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {

	}
}

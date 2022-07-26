package io.tapdata.flow.engine.V2.node.hazelcast.data;


import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.task.context.DataProcessorContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

public class HazelcastVirtualTargetNode extends HazelcastDataBaseNode {

	private final static Logger logger = LogManager.getLogger(HazelcastVirtualTargetNode.class);


	public HazelcastVirtualTargetNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	public void process(int ordinal, @NotNull Inbox inbox) {

	}
}

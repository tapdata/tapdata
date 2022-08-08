package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;

public class HazelcastCacheTarget extends HazelcastTaskTarget {

	public HazelcastCacheTarget(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		Connections sourceConn = dataProcessorContext.getSourceConn();
		Node<?> node = dataProcessorContext.getNode();
		Connections targetConn = Connections.cacheConnection(sourceConn, HazelcastUtil.node2Stages(node));
		this.dataProcessorContext = DataProcessorContext.newBuilder()
				.withTaskDto(dataProcessorContext.getTaskDto())
				.withNode(node)
				.withNodes(dataProcessorContext.getNodes())
				.withEdges(dataProcessorContext.getEdges())
				.withSourceConn(sourceConn)
				.withTargetConn(targetConn)
				.withConfigurationCenter(dataProcessorContext.getConfigurationCenter())
				.withCacheService(dataProcessorContext.getCacheService())
				.build();
	}
}

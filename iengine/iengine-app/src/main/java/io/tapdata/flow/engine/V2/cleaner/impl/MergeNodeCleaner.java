package io.tapdata.flow.engine.V2.cleaner.impl;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import io.tapdata.utils.AppType;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.cleaner.BaseTaskCleaner;
import io.tapdata.flow.engine.V2.cleaner.CleanResult;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastMergeNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.LinkedList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-01-03 12:28
 **/
public class MergeNodeCleaner extends BaseTaskCleaner {

	@Override
	public CleanResult cleanTaskNode(String taskId, String nodeId) {
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		Query query = new Query(Criteria.where("_id").is(taskId));
		query.fields().include("dag");
		TaskDto taskDto = clientMongoOperator.findOne(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
		if (null == taskDto) {
			return CleanResult.success();
		}
		DAG dag = taskDto.getDag();
		List<Node> nodes = dag.getNodes();
		LinkedList<Edge> edges = dag.getEdges();
		List<MergeTableNode> mergeTableNodes;
		if (StringUtils.isBlank(nodeId)) {
			mergeTableNodes = findNodes(nodes, MergeTableNode.class);
		} else {
			Node<?> node = dag.getNode(nodeId);
			if (!(node instanceof MergeTableNode)) {
				return CleanResult.success();
			}
			mergeTableNodes = new LinkedList<>();
			mergeTableNodes.add((MergeTableNode) node);
		}
		for (MergeTableNode mergeTableNode : mergeTableNodes) {
			if (AppType.currentType().isCloud()) {
				HazelcastMergeNode.clearCache(mergeTableNode, nodes, edges);
			} else {
				HazelcastMergeNode.clearCache(mergeTableNode);
			}
		}

		return CleanResult.success();
	}
}

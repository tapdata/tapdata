package io.tapdata.flow.engine.V2.cleaner.impl;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.cleaner.BaseTaskCleaner;
import io.tapdata.flow.engine.V2.cleaner.CleanResult;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastMergeNode;
import io.tapdata.utils.AppType;
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

	protected TaskDto findTaskById(String taskId) {
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		Query query = new Query(Criteria.where("_id").is(taskId));
		query.fields().include("dag");
		return clientMongoOperator.findOne(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
	}

	protected MergeTableNode getMergeTableNode(DAG dag, String nodeId) {
		Node<?> node = dag.getNode(nodeId);
		if (node instanceof MergeTableNode) {
			return (MergeTableNode) node;
		}
		return null;
	}

	protected void cleanTaskNodeByAppType(List<MergeTableNode> mergeTableNodes, DAG dag) {
		if (AppType.currentType().isCloud()) {
			for (MergeTableNode mergeTableNode : mergeTableNodes) {
				HazelcastMergeNode.clearCache(mergeTableNode, dag.getNodes(), dag.getEdges());
			}
		} else {
			for (MergeTableNode mergeTableNode : mergeTableNodes) {
				HazelcastMergeNode.clearCache(mergeTableNode);
			}
		}
	}

	@Override
	public CleanResult cleanTaskNode(String taskId, String nodeId) {
		TaskDto taskDto = findTaskById(taskId);
		if (null == taskDto) {
			return CleanResult.success();
		}
		DAG dag = taskDto.getDag();
		List<MergeTableNode> mergeTableNodes;
		if (StringUtils.isBlank(nodeId)) {
			List<Node> nodes = dag.getNodes();
			mergeTableNodes = findNodes(nodes, MergeTableNode.class);
		} else {
			MergeTableNode mergeTableNode = getMergeTableNode(dag, nodeId);
			if (null == mergeTableNode) {
				return CleanResult.success();
			}
			mergeTableNodes = new LinkedList<>();
			mergeTableNodes.add(mergeTableNode);
		}

		cleanTaskNodeByAppType(mergeTableNodes, dag);

		return CleanResult.success();
	}
}

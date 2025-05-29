package io.tapdata.flow.engine.V2.util;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.node.NodeTypeEnum;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-11-22 14:20
 **/
public class ProcessAfterMergeUtil {

	public static final String NODE_ID_SUFFIX = "tpv";
	public static final String TABLE_NAME_PREFIX = "process_after_merge";

	public static List<TableNode> handleDagWhenProcessAfterMerge(TaskDto taskDto) {
		com.tapdata.tm.commons.dag.DAG tmDag = taskDto.getDag();
		List<Node> nodes = tmDag.getNodes();
		List<TableNode> addedNodes = new ArrayList<>();
		if (CollectionUtils.isEmpty(nodes)) {
			return addedNodes;
		}
		List<Node> mergeNodes = GraphUtil.findNodes(taskDto, MergeTableNode.class);
		if (CollectionUtils.isEmpty(mergeNodes)) {
			return addedNodes;
		}
		if (!taskDto.isNormalTask()) {
			return addedNodes;
		}
		// find if there's successors that is not table node after merge node
		for (Node mergeNode : mergeNodes) {
			List<Node> successors = mergeNode.successors();
			if (CollectionUtils.isEmpty(successors)) {
				continue;
			}
			for (Node successor : successors) {
				if (NodeTypeEnum.TABLE.type.equals(successor.getType())) {
					continue;
				}
				List<Node<?>> targetTableNodes = GraphUtil.successors(successor, n -> NodeTypeEnum.TABLE.type.equals(n.getType()));
				if (CollectionUtils.isEmpty(targetTableNodes)) {
					continue;
				}
				for (Node<?> targetTableNode : targetTableNodes) {
					TableNode tableNode = new TableNode();
					BeanUtils.copyProperties(targetTableNode, tableNode);
					tableNode.setId(String.join("_", tableNode.getId(), NODE_ID_SUFFIX));
					String tableName = tableNode.getTableName();
					tableName = String.join("_", TABLE_NAME_PREFIX, tableName, tableNode.getId());
					tableNode.setName(tableName);
					tableNode.setTableName(tableName);
					tableNode.setSourceAndTarget(true);
					tableNode.setIgnoreMetrics(true);
					
					tmDag.addTargetNode(mergeNode, tableNode);
					addedNodes.add(tableNode);
				}
			}
		}
		return addedNodes;
	}
}

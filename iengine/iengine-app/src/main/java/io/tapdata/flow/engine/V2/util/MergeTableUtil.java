package io.tapdata.flow.engine.V2.util;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Mapping;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastTaskSourceAndTarget;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastTaskTarget;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-03-05 00:03
 **/

public class MergeTableUtil {

	/**
	 * Find all target data node from merge table node
	 *
	 * @param nodes node list
	 * @param edges edge list
	 * @return key: target data node id, value: merge table node
	 * e.g. edge: {source: "111", target: "222"}, result: {"222": {id: "111", type: "mergeTable"}}
	 */
	public static Map<String, MergeTableNode> getMergeTableMap(List<Node> nodes, List<Edge> edges) throws Exception {
		Map<String, MergeTableNode> mergeTableMap = new HashMap<>();

		try {
			for (Node<?> node : nodes) {
				try {
					if (node instanceof MergeTableNode) {
						Edge findEdge = edges.stream().filter(edge -> edge.getSource().equals(node.getId())).findFirst().orElse(null);
						if (null == findEdge) {
							throw new RuntimeException("Found node " + getNodeDesc(node) + " dont have an edge");
						}
						String target = findEdge.getTarget();
						Node<?> nextNode = nodes.stream().filter(n -> n.getId().equals(target)).findFirst().orElse(null);
						if (null == nextNode) {
							throw new RuntimeException("Found next node not exists, current node: " + getNodeDesc(node) + ", next node id: " + target);
						}
						if (!nextNode.isDataNode()) {
							throw new RuntimeException("Found next node is not a data node: " + getNodeDesc(nextNode));
						}
						if (mergeTableMap.containsKey(nextNode.getId())) {
							throw new RuntimeException("Multiple merge nodes are found linking this data node: " + getNodeDesc(nextNode));
						}
						mergeTableMap.put(nextNode.getId(), (MergeTableNode) node);
					}
				} catch (Throwable e) {
					throw new RuntimeException("Occur an error when handle merge table node " + getNodeDesc(node) + "; " + e.getMessage(), e);
				}
			}
		} catch (Exception e) {
			throw new Exception("Handle merge table node occurred an exception: " + e.getMessage(), e);
		}

		return mergeTableMap;
	}

	public static void setMergeTableIntoHZTarget(Map<String, MergeTableNode> mergeTableMap, HazelcastBaseNode targetNode) {
		if (MapUtils.isEmpty(mergeTableMap)) {
			return;
		}
		HazelcastTaskTarget hazelcastTaskTarget = null;
		if (targetNode instanceof HazelcastTaskTarget) {
			hazelcastTaskTarget = (HazelcastTaskTarget) targetNode;
		} else if (targetNode instanceof HazelcastTaskSourceAndTarget) {
			hazelcastTaskTarget = ((HazelcastTaskSourceAndTarget) targetNode).getTarget();
		}
		if (null == hazelcastTaskTarget) {
			return;
		}
		MergeTableNode mergeTableNode = mergeTableMap.getOrDefault(hazelcastTaskTarget.getNode().getId(), null);
		if (null != mergeTableNode) {
			hazelcastTaskTarget.setMergeTableNode(mergeTableNode);
		}
	}

	public static void mergeTablePropertyFillInMappings(MergeTableNode mergeTableNode, List<Mapping> mappings, List<Node> nodes) {
		if (null == mergeTableNode || CollectionUtils.isEmpty(mappings)) {
			return;
		}
		List<MergeTableProperties> properties = mergeTableNode.getMergeProperties();
		if (CollectionUtils.isEmpty(properties)) {
			return;
		}
		for (MergeTableProperties property : properties) {
			String sourceId = property.getId();
			if (StringUtils.isBlank(sourceId)) {
				throw new RuntimeException("Merge table node's source id is blank");
			}
			Node preNode = nodes.stream().filter(n -> n.getId().equals(sourceId)).findFirst().orElse(null);
			if (null == preNode) {
				throw new RuntimeException("Cannot found pre node by merge table node source id: " + sourceId);
			}
			List<Node<?>> predecessors = new ArrayList<>();
			predecessors.add(preNode);
			predecessors = GraphUtil.predecessors(mergeTableNode, Node::isDataNode, predecessors);
			if (predecessors.isEmpty()) {
				throw new RuntimeException("Cannot found pre data node by merge table node source id: " + sourceId);
			}
			if (predecessors.size() > 1) {
				throw new RuntimeException("Found multiple pre data node by merge table node source id: " + sourceId + ", should be one");
			}
			Node<?> preDataNode = predecessors.get(0);
			if (!(preDataNode instanceof TableNode)) {
				throw new RuntimeException("Found pre data node isn't a table node by merge table node source id: " + sourceId + ", node type: " + preDataNode.getClass().getSimpleName());
			}
			String tableName = ((TableNode) preDataNode).getTableName();
			if (StringUtils.isBlank(tableName)) {
				throw new RuntimeException("Found pre data node's table name is blank");
			}
			Mapping mapping = mappings.parallelStream().filter(m -> m.getFrom_table().equals(tableName)).findFirst().orElse(null);
			if (null == mapping) {
				throw new RuntimeException("Merge table node cannot find a mapping by table name: " + tableName);
			}
			MergeTableProperties.MergeType mergeType = property.getMergeType();
			switch (mergeType) {
				case appendWrite:
					mapping.setRelationship(ConnectorConstant.RELATIONSHIP_APPEND);
					break;
				case updateOrInsert:
					mapping.setRelationship(ConnectorConstant.RELATIONSHIP_ONE_ONE);
					mapping.setJoin_condition(joinConditionConverter(property.getJoinKeys()));
					break;
				case updateWrite:
					mapping.setRelationship(ConnectorConstant.RELATIONSHIP_ONE_MANY);
					mapping.setJoin_condition(joinConditionConverter(property.getJoinKeys()));
					break;
				case updateIntoArray:
					mapping.setRelationship(ConnectorConstant.RELATIONSHIP_MANY_ONE);
					mapping.setJoin_condition(joinConditionConverter(property.getJoinKeys()));
					mapping.setMatch_condition(matchConditionConverter(property.getArrayKeys()));
					break;
			}
			mapping.setTarget_path(property.getTargetPath());
		}
	}

	private static List<Map<String, String>> joinConditionConverter(List<Map<String, String>> list) {
		List<Map<String, String>> result = new ArrayList<>();
		if (CollectionUtils.isEmpty(list)) {
			return result;
		}
		for (Map<String, String> obj : list) {
			Map<String, String> map = new HashMap<>();
			map.put(obj.getOrDefault("source", ""), obj.getOrDefault("target", ""));
			result.add(map);
		}
		return result;
	}

	private static List<Map<String, String>> matchConditionConverter(List<String> list) {
		if (CollectionUtils.isEmpty(list)) {
			return new ArrayList<>();
		}
		List<Map<String, String>> result = new ArrayList<>();
		for (String str : list) {
			Map<String, String> map = new HashMap<>();
			map.put(str, str);
			result.add(map);
		}
		return result;
	}

	private static String getNodeDesc(Node<?> node) {
		return node.getName() + "(id: " + node.getId() + ", type: " + node.getType() + ")";
	}
}

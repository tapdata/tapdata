package com.tapdata.tm.lineage.analyzer.impl;

import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.lineage.analyzer.AnalyzeLayer;
import com.tapdata.tm.lineage.analyzer.BaseAnalyzer;
import com.tapdata.tm.lineage.analyzer.entity.LineageAttr;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.lineage.analyzer.entity.LineageModuleNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageModules;
import com.tapdata.tm.lineage.analyzer.entity.LineageNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode;
import com.tapdata.tm.lineage.entity.LineageType;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import io.github.openlg.graphlib.Graph;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2023-05-22 14:21
 **/
@Service("tableAnalyzerV1")
public class TableAnalyzerV1 extends BaseAnalyzer {

	private static final String[] TASK_INCLUDE_FIELDS = new String[]{"_id", "name", "dag", "syncType", "status"};
	private static final String[] DATASOURCE_INCLUDE_FIELDS = new String[]{"_id", "name", "pdkType", "pdkHash"};
	private static final String[] MODULES_INCLUDE_FIELDS = new String[]{"_id", "name", "datasource", "tableName", "basePath", "status", "listtags"};
	public static final String[] METADATA_INCLUDE_FIELDS = new String[]{"_id", "sourceType"};
	private final Map<String, DataSourceEntity> dataSourceEntityMap = new ConcurrentHashMap<>();
	private final Map<String, Set<String>> analyzedTaskIdMap = new ConcurrentHashMap<>();
	private final Map<String, Map<String, List<TaskEntity>>> foundedTask = new ConcurrentHashMap<>();

	@Override
	public Graph<Node, Edge> analyzeTable(String connectionId, String table, LineageType lineageType) throws Exception {
		AnalyzeLayer analyzeLayer;
		try {
			Graph<Node, Edge> graph = new Graph<>(true, true, false);
			analyzeLayer = initAnalyzeLayer(connectionId, table, graph);
			List<TaskEntity> tasks = findTasks(connectionId, table, null);
			LineageTableNode lineageTableNode = null;
			if (CollectionUtils.isEmpty(tasks)) {
				DataSourceEntity dataSource = findDataSource(connectionId);
				LineageMetadataInstance metadata = getMetadata(connectionId, table);
				lineageTableNode = new LineageTableNode(table, connectionId, dataSource.getName(), dataSource.getPdkHash(), metadata);
				setGraphNode(graph, lineageTableNode);
			} else {
				for (TaskEntity taskEntity : tasks) {
					if (null == taskEntity.getId()) {
						continue;
					}
					if (checkTaskIsAnalyzed(taskEntity.getId().toHexString())) {
						continue;
					}
					Node node = findNodeInTask(taskEntity, connectionId, table);
					if (null == node) {
						continue;
					}
					LineageTask lineageTask = wrapLineageTask(taskEntity, node);
					analyzeLayer.setPreNode(node);
					analyzeLayer.setPreInvalidNode(node);
					DataSourceEntity dataSource = findDataSource(connectionId);
					lineageTableNode = new LineageTableNode(table, dataSource.getId().toHexString(), dataSource.getName(), dataSource.getPdkHash(), getMetadata(connectionId, table))
							.addTask(lineageTask);
					setGraphNode(analyzeLayer, lineageTask, node);
					analyzeLayer.setPreLineageTableNode(lineageTableNode);
					analyzeLayer.setCurrentTask(lineageTask);
					analyzeLayer.getNotInTaskIds().add(lineageTask.getId());
					if (LineageType.ALL_STREAM == lineageType) {
						analyzeLayer.setLineageType(LineageType.UPSTREAM);
						recursiveAnalyze(analyzeLayer);
						analyzeLayer = initAnalyzeLayer(connectionId, table, graph);
						analyzeLayer.setPreNode(node);
						analyzeLayer.setPreInvalidNode(node);
						analyzeLayer.setPreLineageTableNode(lineageTableNode);
						analyzeLayer.setCurrentTask(lineageTask);
						analyzeLayer.getNotInTaskIds().add(lineageTask.getId());
						analyzeLayer.setLineageType(LineageType.DOWNSTREAM);
					} else {
						analyzeLayer.setLineageType(lineageType);
					}
					recursiveAnalyze(analyzeLayer);
					if (null != taskEntity.getId()) {
						addAnalyzedTaskId(taskEntity.getId().toHexString());
					}
				}
			}

			if (null != lineageTableNode) {
				analyzeApiserver(analyzeLayer, lineageTableNode);
			}
		} finally {
			analyzedTaskIdMap.remove(Thread.currentThread().getName());
			foundedTask.remove(Thread.currentThread().getName());
		}
		return analyzeLayer.getGraph();
	}

	@NotNull
	private static LineageTaskNode wrapLineageTaskNode(Node node) {
		LineageTaskNode lineageTaskNode = new LineageTaskNode(node.getId(), node.getName(), node.getType());
		if (CollectionUtils.isEmpty(node.predecessors())) {
			lineageTaskNode.setTaskNodePos(LineageTaskNode.TASK_NODE_SOURCE_POS);
		} else if (CollectionUtils.isEmpty(node.successors())) {
			lineageTaskNode.setTaskNodePos(LineageTaskNode.TASK_NODE_TARGET_POS);
		}
		return lineageTaskNode;
	}

	@NotNull
	private static AnalyzeLayer initAnalyzeLayer(String connectionId, String table, Graph<Node, Edge> graph) {
		AnalyzeLayer analyzeLayer = new AnalyzeLayer();
		analyzeLayer.setGraph(graph);
		analyzeLayer.setConnectionId(connectionId);
		analyzeLayer.setTable(table);
		analyzeLayer.setNotInTaskIds(new HashSet<>());
		return analyzeLayer;
	}

	@NotNull
	private static AnalyzeLayer initAnalyzeLayer(String connectionId, String table, Graph<Node, Edge> graph, Set<String> notInTaskIds) {
		AnalyzeLayer analyzeLayer = new AnalyzeLayer();
		analyzeLayer.setGraph(graph);
		analyzeLayer.setConnectionId(connectionId);
		analyzeLayer.setTable(table);
		analyzeLayer.setNotInTaskIds(new HashSet<>(notInTaskIds));
		return analyzeLayer;
	}

	private void recursiveAnalyze(AnalyzeLayer analyzeLayer) {
		Node preNode = analyzeLayer.getPreNode();
		LineageType lineageType = analyzeLayer.getLineageType();

		if (null == preNode) return;
		List<Node> nextNodes;
		if (LineageType.DOWNSTREAM == lineageType) {
			nextNodes = preNode.successors();
		} else if (LineageType.UPSTREAM == lineageType) {
			nextNodes = preNode.predecessors();
		} else {
			return;
		}
		if (CollectionUtils.isNotEmpty(nextNodes)) {
			for (Node nextNode : nextNodes) {
				LineageTask lineageTask = wrapLineageTask(analyzeLayer.getCurrentTask(), nextNode);
				analyzeAndSetGraphNodeAndEdge(analyzeLayer, nextNode, lineageTask);
			}
		} else {
			List<TaskEntity> tasks = findTasks(analyzeLayer.getConnectionId(), analyzeLayer.getTable(), analyzeLayer.getNotInTaskIds());
			if (CollectionUtils.isEmpty(tasks)) {
				return;
			}
			tasks = taskFilter(tasks, analyzeLayer);
			for (TaskEntity task : tasks) {
				Node nodeInTask = findNodeInTask(task, analyzeLayer.getConnectionId(), analyzeLayer.getTable());
				LineageTask lineageTask = wrapLineageTask(task, nodeInTask);
				if (null != task.getId()) {
					analyzeLayer.getNotInTaskIds().add(task.getId().toHexString());
				}
				analyzeAndSetGraphNodeAndEdge(analyzeLayer, nodeInTask, lineageTask);
				if (null != task.getId()) {
					addAnalyzedTaskId(task.getId().toHexString());
				}
			}
		}
	}

	private void analyzeAndSetGraphNodeAndEdge(AnalyzeLayer analyzeLayer, Node nextNode, LineageTask lineageTask) {
		LineageType lineageType = analyzeLayer.getLineageType();
		AnalyzeLayer nextLayer = null;
		if (nextNode instanceof TableNode) {
			nextLayer = analyzeTableNode((TableNode) nextNode, analyzeLayer.getGraph(), analyzeLayer);
		} else if (nextNode instanceof DatabaseNode) {
			nextLayer = analyzeDatabaseNode((DatabaseNode) nextNode, analyzeLayer.getGraph(), analyzeLayer, analyzeLayer.getPreInvalidNode(), analyzeLayer.getTable(), analyzeLayer.getLineageType());
		}
		if (null != nextLayer) {
			nextLayer.setLineageType(lineageType);
			nextLayer.setPreNode(nextNode);
			nextLayer.setPreInvalidNode(nextNode);
			nextLayer.setCurrentTask(lineageTask);
			LineageTableNode nextLineageTableNode = setGraphNode(nextLayer, lineageTask, nextNode);
			nextLayer.setPreLineageTableNode(nextLineageTableNode);
			if (LineageType.UPSTREAM == lineageType) {
				setGraphEdge(nextLayer.getGraph(), nextLineageTableNode, analyzeLayer.getPreLineageTableNode(), nextLayer.getCurrentTask());
			} else if (LineageType.DOWNSTREAM == lineageType) {
				setGraphEdge(nextLayer.getGraph(), analyzeLayer.getPreLineageTableNode(), nextLineageTableNode, nextLayer.getCurrentTask());
				analyzeApiserver(nextLayer, nextLineageTableNode);
			}
			recursiveAnalyze(nextLayer);
		} else {
			analyzeLayer.setPreNode(nextNode);
			recursiveAnalyze(analyzeLayer);
		}
	}

	private void analyzeApiserver(AnalyzeLayer analyzeLayer, LineageNode preNode) {
		List<ModulesEntity> modules = findModules(analyzeLayer.getConnectionId(), analyzeLayer.getTable());
		if (CollectionUtils.isNotEmpty(modules)) {
			for (ModulesEntity module : modules) {
				String appName = "";
				List<Tag> listtags = module.getListtags();
				if (CollectionUtils.isNotEmpty(listtags) && listtags.size() > 0) {
					Tag tag = listtags.get(0);
					appName = tag.getValue();
				}
				LineageModules lineageModules = new LineageModules(
						module.getId().toHexString(),
						module.getName(),
						module.getDataSource(),
						module.getTableName(),
						module.getBasePath(),
						module.getStatus(),
						appName
				);
				LineageModuleNode lineageModuleNode = setGraphModulesNode(analyzeLayer, lineageModules);
				setGraphEdge(analyzeLayer.getGraph(), preNode, lineageModuleNode, lineageModules);
			}
		}
	}

	private List<TaskEntity> taskFilter(List<TaskEntity> tasks, AnalyzeLayer analyzeLayer) {
		LineageType lineageType = analyzeLayer.getLineageType();
		tasks = tasks.stream().filter(task -> {
			Node nodeInTask = findNodeInTask(task, analyzeLayer.getConnectionId(), analyzeLayer.getTable());
			if (null == nodeInTask) return false;
			if (LineageType.UPSTREAM == lineageType) {
				return CollectionUtils.isEmpty(nodeInTask.successors());
			} else if (LineageType.DOWNSTREAM == lineageType) {
				return CollectionUtils.isEmpty(nodeInTask.predecessors());
			} else {
				return false;
			}
		}).collect(Collectors.toList());
		return tasks;
	}

	private AnalyzeLayer analyzeTableNode(TableNode tableNode, Graph<Node, Edge> graph, AnalyzeLayer preLayer) {
		return initAnalyzeLayer(tableNode.getConnectionId(), tableNode.getTableName(), graph, preLayer.getNotInTaskIds());
	}

	private AnalyzeLayer analyzeDatabaseNode(DatabaseNode databaseNode, Graph<Node, Edge> graph, AnalyzeLayer preLayer, Node preNode, String preTable, LineageType lineageType) {
		LinkedHashMap<String, String> tableNameRelation;
		String tableName = null;
		switch (lineageType) {
			case DOWNSTREAM:
				tableNameRelation = findTableNameRelation(databaseNode.getSyncObjects());
				if (tableNameRelation == null) {
					tableName = preTable;
				} else {
					tableName = tableNameRelation.get(preTable);
				}
				break;
			case UPSTREAM:
				if (preNode instanceof TableNode) {
					tableName = ((TableNode) preNode).getTableName();
				} else if (preNode instanceof DatabaseNode) {
					DatabaseNode preDatabaseNode = (DatabaseNode) preNode;
					tableNameRelation = findTableNameRelation(preDatabaseNode.getSyncObjects());
					if (null == tableNameRelation) return null;
					if (StringUtils.isNotBlank(preTable)) {
						Map.Entry<String, String> entry = tableNameRelation.entrySet().stream().filter(e -> e.getValue().equals(preTable)).findFirst().orElse(null);
						if (null == entry) return null;
						tableName = entry.getKey();
					}
				}
				break;
		}
		if (StringUtils.isNotBlank(tableName)) {
			return initAnalyzeLayer(databaseNode.getConnectionId(), tableName, graph, preLayer.getNotInTaskIds());
		} else {
			return null;
		}
	}

	@Nullable
	private static LinkedHashMap<String, String> findTableNameRelation(List<SyncObjects> syncObjects) {
		if (CollectionUtils.isEmpty(syncObjects)) return null;
		SyncObjects objects = syncObjects.stream().filter(so -> MapUtils.isNotEmpty(so.getTableNameRelation())).findFirst().orElse(null);
		if (null == objects) return null;
		return objects.getTableNameRelation();
	}

	private List<TaskEntity> findTasks(String connectionId, String table, Set<String> notInTaskIds) {
		List<TaskEntity> foundedTasks = findFoundedTasks(connectionId, table);
		if (null != foundedTasks) {
			if (CollectionUtils.isNotEmpty(notInTaskIds)) {
				return foundedTasks.stream().filter(t -> !notInTaskIds.contains(t.getId().toHexString())).collect(Collectors.toList());
			}
			return foundedTasks;
		}
		Criteria taskCriteria = buildTaskCriteria(connectionId, table);
		Query query;
		if (CollectionUtils.isNotEmpty(notInTaskIds)) {
			List<ObjectId> notInObjIds = notInTaskIds.stream().map(ObjectId::new).collect(Collectors.toList());
			taskCriteria = taskCriteria.and("_id").nin(notInObjIds);
		}
		query = Query.query(taskCriteria);
		taskQueryFields(query);
		List<TaskEntity> taskEntities = taskRepository.findAll(query);
		addFoundedTask(connectionId, table, taskEntities);
		return taskRepository.findAll(query);
	}

	@NotNull
	private static Criteria buildTaskCriteria(String connectionId, String table) {
		Criteria syncTaskCriteria = new Criteria("dag.nodes.connectionId").is(connectionId).and("dag.nodes.tableName").is(table);
		Criteria migrateSrcCriteria = new Criteria("dag.nodes.tableNames").is(table);
		Criteria migrateTgtCriteria = new Criteria("dag.nodes.syncObjects.objectNames").is(table);
		Criteria migrateCriteria = new Criteria("dag.nodes.connectionId").is(connectionId)
				.andOperator(new Criteria().orOperator(migrateSrcCriteria, migrateTgtCriteria));
		Criteria notDeleteCriteria = new Criteria("is_deleted").is(false);
		return new Criteria().andOperator(
				notDeleteCriteria,
				new Criteria().orOperator(syncTaskCriteria, migrateCriteria)
		);
	}

	private static void taskQueryFields(Query query) {
		query.fields().include(TASK_INCLUDE_FIELDS);
	}

	private Node findNodeInTask(TaskEntity task, String connectionId, String table) {
		if (null == task) {
			return null;
		}
		List<Node> nodes = task.getDag().getNodes();
		return nodes.stream().filter(node -> {
			if (node instanceof TableNode) {
				TableNode tableNode = (TableNode) node;
				return connectionId.equals(tableNode.getConnectionId()) && table.equals(tableNode.getTableName());
			} else if (node instanceof DatabaseNode) {
				DatabaseNode databaseNode = (DatabaseNode) node;
				if (!connectionId.equals(databaseNode.getConnectionId())) {
					return false;
				}
				List<String> tableNames = databaseNode.getTableNames();
				if (CollectionUtils.isNotEmpty(tableNames)) {
					return tableNames.contains(table);
				}
				List<SyncObjects> syncObjects = databaseNode.getSyncObjects();
				if (CollectionUtils.isNotEmpty(syncObjects)) {
					SyncObjects objects = syncObjects.stream().filter(so -> CollectionUtils.isNotEmpty(so.getObjectNames())).findFirst().orElse(null);
					if (null != objects) {
						return objects.getObjectNames().contains(table);
					}
				}
				return false;
			} else {
				return false;
			}
		}).findFirst().orElse(null);
	}

	@NotNull
	private DataSourceEntity findDataSource(String id) {
		DataSourceEntity dataSource;
		if (StringUtils.isBlank(id)) {
			dataSource = new DataSourceEntity();
			dataSource.setName("data source id is blank");
			return dataSource;
		}
		dataSource = dataSourceEntityMap.computeIfAbsent(id, k -> {
			Query query = Query.query(Criteria.where("_id").is(new ObjectId(k)));
			query.fields().include(DATASOURCE_INCLUDE_FIELDS);
			return dataSourceRepository.findOne(query).orElse(null);
		});
		if (null == dataSource) {
			dataSource = new DataSourceEntity();
			dataSource.setId(new ObjectId(id));
			dataSource.setName(String.format("%s not exists", id));
		}
		return dataSource;
	}

	private LineageTableNode setGraphNode(AnalyzeLayer analyzeLayer, LineageTask task, Node node) {
		if (null == task || null == node) {
			return null;
		}
		LineageTask lineageTask = wrapLineageTask(task, node);
		Graph<Node, Edge> graph = analyzeLayer.getGraph();
		Node graphNode = graph.getNode(LineageNode.genId(LineageTableNode.NODE_TYPE, analyzeLayer.getConnectionId(), analyzeLayer.getTable()));
		LineageTableNode lineageTableNode;
		if (graphNode instanceof LineageTableNode) {
			lineageTableNode = (LineageTableNode) graphNode;
			lineageTableNode.addTask(lineageTask);
		} else {
			DataSourceEntity dataSource = findDataSource(analyzeLayer.getConnectionId());
			lineageTableNode = new LineageTableNode(analyzeLayer.getTable(), analyzeLayer.getConnectionId(), dataSource.getName(), dataSource.getPdkHash(), getMetadata(analyzeLayer.getConnectionId(), analyzeLayer.getTable()));
			lineageTableNode.addTask(lineageTask);
			setGraphNode(graph, lineageTableNode);
		}
		return lineageTableNode;
	}

	private LineageMetadataInstance getMetadata(String connectionId, String tableName) {
		Criteria baseCriteria = new Criteria("source._id").is(connectionId)
				.and("original_name").is(tableName);
		Criteria sourceCriteria = new Criteria("sourceType").is(SourceTypeEnum.SOURCE.name());
		Criteria virtualCriteria = new Criteria("sourceType").is(SourceTypeEnum.VIRTUAL.name());
		Query query = Query.query(new Criteria().andOperator(baseCriteria, sourceCriteria));
		query.fields().include(METADATA_INCLUDE_FIELDS);
		LineageMetadataInstance lineageMetadataInstance = getMetadata(query);
		if (null == lineageMetadataInstance) {
			query = Query.query(new Criteria().andOperator(baseCriteria, virtualCriteria));
			query.fields().include(METADATA_INCLUDE_FIELDS);
			lineageMetadataInstance = getMetadata(query);
		}
		return lineageMetadataInstance;
	}

	private LineageMetadataInstance getMetadata(Query query) {
		MetadataInstancesEntity metadataInstancesEntity = metadataInstancesRepository.findOne(query).orElse(null);
		if (null == metadataInstancesEntity || null == metadataInstancesEntity.getId()) return null;
		LineageMetadataInstance lineageMetadataInstance = new LineageMetadataInstance();
		lineageMetadataInstance.setId(metadataInstancesEntity.getId().toHexString());
		lineageMetadataInstance.setSourceType(metadataInstancesEntity.getSourceType());
		return lineageMetadataInstance;
	}

	private LineageModuleNode setGraphModulesNode(AnalyzeLayer analyzeLayer, LineageModules lineageModules) {
		if (null == lineageModules) {
			return null;
		}
		LineageModuleNode lineageModuleNode = new LineageModuleNode(analyzeLayer.getTable(), analyzeLayer.getConnectionId(), lineageModules);
		setGraphNode(analyzeLayer.getGraph(), lineageModuleNode);
		/*Node graphNode = analyzeLayer.getGraph().getNode(LineageNode.genId(LineageModuleNode.NODE_TYPE, analyzeLayer.getConnectionId(), analyzeLayer.getTable()));
		LineageModuleNode lineageModuleNode;
		if (graphNode instanceof LineageModuleNode) {
			lineageModuleNode = (LineageModuleNode) graphNode;
			lineageModuleNode.addModule(lineageModules);
		} else {
			lineageModuleNode = new LineageModuleNode(analyzeLayer.getTable(), analyzeLayer.getConnectionId());
			lineageModuleNode.addModule(lineageModules);
			setGraphNode(analyzeLayer.getGraph(), lineageModuleNode);
		}*/
		return lineageModuleNode;
	}

	private void setGraphEdge(Graph<Node, Edge> graph, LineageNode node1, LineageNode node2, LineageAttr lineageAttr) {
		if (node1.getId().equals(node2.getId())) {
			return;
		}
		Edge existEdge = graph.getEdge(node1.getId(), node2.getId());
		if (null == existEdge) {
			Map<String, Object> attrs = new HashMap<>();
			Map<String, LineageAttr> lineageAttrMap = new HashMap<>();
			lineageAttrMap.put(lineageAttr.getId(), lineageAttr);
			attrs.put(lineageAttr.getAttrKey(), lineageAttrMap);
			Edge edge = new Edge(lineageAttr.getName(), attrs, node1.getId(), node2.getId());
			graph.setEdge(node1.getId(), node2.getId(), edge);
		} else {
			Map<String, Object> attrs = existEdge.getAttrs();
			Map.Entry<String, Object> entry = attrs.entrySet().stream().filter(e -> e.getKey().equals(lineageAttr.getAttrKey())).findFirst().orElse(null);
			if (null != entry) {
				Object value = entry.getValue();
				if (value instanceof Map) {
					((Map<String, LineageAttr>) value).put(lineageAttr.getId(), lineageAttr);
				}
			}
		}
	}

	private void setGraphNode(Graph<Node, Edge> graph, LineageNode lineageNode) {
		if (null == graph || null == lineageNode) return;
		graph.setNode(lineageNode.getId(), lineageNode);
	}

	@NotNull
	private static LineageTask wrapLineageTask(LineageTask task, Node node) {
		return new LineageTask(
				task.getId(),
				task.getName(),
				wrapLineageTaskNode(node),
				task.getSyncType(),
				task.getStatus()
		);
	}

	@NotNull
	private static LineageTask wrapLineageTask(TaskEntity task, Node node) {
		return new LineageTask(
				task.getId().toHexString(),
				task.getName(),
				wrapLineageTaskNode(node),
				task.getSyncType(),
				task.getStatus()
		);
	}

	private List<ModulesEntity> findModules(String connectionId, String table) {
		Criteria criteria = new Criteria("datasource").is(connectionId)
				.and("tableName").is(table);
		Query query = Query.query(criteria);
		query.fields().include(MODULES_INCLUDE_FIELDS);
		return modulesRepository.findAll(query);
	}

	private void addAnalyzedTaskId(String taskId) {
		String name = Thread.currentThread().getName();
		analyzedTaskIdMap.computeIfAbsent(name, k -> new HashSet<String>() {{
			add(taskId);
		}});
		analyzedTaskIdMap.computeIfPresent(name, (k, v) -> {
			v.add(taskId);
			return v;
		});
	}

	private boolean checkTaskIsAnalyzed(String taskId) {
		String name = Thread.currentThread().getName();
		return analyzedTaskIdMap.containsKey(name) && analyzedTaskIdMap.get(name).contains(taskId);
	}

	private void addFoundedTask(String connectionId, String table, List<TaskEntity> tasks) {
		String name = Thread.currentThread().getName();
		String key = String.join("_", connectionId, table);
		if (null == tasks) {
			return;
		}
		foundedTask.computeIfAbsent(name, k -> new HashMap<String, List<TaskEntity>>() {{
			put(key, new ArrayList<>(tasks));
		}});
	}

	private List<TaskEntity> findFoundedTasks(String connectionId, String table) {
		String name = Thread.currentThread().getName();
		String key = String.join("_", connectionId, table);
		Map<String, List<TaskEntity>> map = foundedTask.get(name);
		if (null == map) {
			return null;
		}
		return map.get(key);
	}
}

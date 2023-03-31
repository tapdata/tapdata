package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtilV2;
import com.tapdata.constant.NotExistsNode;
import com.tapdata.entity.Connections;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.HazelcastNotExistsException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.schema.SchemaList;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.tapdata.constant.ConnectorConstant.LOOKUP_TABLE_SUFFIX;

/**
 * @author samuel
 * @Description
 * @create 2022-03-23 11:44
 **/
public class HazelcastMergeNode extends HazelcastProcessorBaseNode {

	public static final String TAG = HazelcastMergeNode.class.getSimpleName();
	private Logger logger = LogManager.getLogger(HazelcastMergeNode.class);

	// 缓存表信息{"前置节点id": "Hazelcast缓存资源{"join value string": {"pk value string": "after data"}}"}
	private Map<String, ConstructIMap<Document>> mergeCacheMap;
	// 合并配置信息{"前置节点id": "合并配置"}
	private Map<String, MergeTableProperties> mergeTablePropertiesMap;
	// 反查信息{"前置节点id": "需要反查的子表配置"}
	private Map<String, List<MergeTableProperties>> lookupMap;
	// 源表节点信息{"前置节点id": "源表节点"}
	private Map<String, Node<?>> sourceNodeMap;
	// 源表连接信息{"前置节点id": "源连接"}
	private Map<String, Connections> sourceConnectionMap;
	// 主键或唯一键信息{"前置节点id": "主键或唯一键列表"}
	private Map<String, List<String>> sourcePkOrUniqueFieldMap;
	// 存储所有需要反查的节点id，提高判断事件是否需要缓存的效率
	private List<String> needCacheIdList;
	// Create index events for target
	private TapdataEvent createIndexEvent;

	public HazelcastMergeNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		initMergeTableProperties(null);
		initLookupMergeProperties();
		initMergeCache();
		initSourceNodeMap(null);
		initSourceConnectionMap(null);
		initSourcePkOrUniqueFieldMap(null);

		TapCreateIndexEvent mergeConfigCreateIndexEvent = generateCreateIndexEventsForTarget();
		this.createIndexEvent = new TapdataEvent();
		this.createIndexEvent.setTapEvent(mergeConfigCreateIndexEvent);
	}

	@Override
	protected void updateNodeConfig(TapdataEvent tapdataEvent) {
		super.updateNodeConfig(tapdataEvent);
		initMergeTableProperties(null);
		initLookupMergeProperties();
		initMergeCache();
		initSourceNodeMap(null);
		initSourceConnectionMap(null);
		initSourcePkOrUniqueFieldMap(null);
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		if (this.createIndexEvent != null) {
			consumer.accept(this.createIndexEvent, null);
			this.createIndexEvent = null;
		}
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		if (!tapdataEvent.isDML()) {
			consumer.accept(tapdataEvent, null);
			return;
		}
		String preNodeId = getPreNodeId(tapdataEvent);
		Node<?> preNode = processorBaseContext.getNodes().stream().filter(n -> n.getId().equals(preNodeId)).findFirst().orElse(null);
		if (null == preNode) throw new RuntimeException("Cannot found node, id: " + preNodeId);
		String preTableName;
		if (preNode instanceof TableNode) {
			preTableName = ((TableNode) preNode).getTableName();
		} else {
			preTableName = preNodeId;
		}
		if (needCache(tapdataEvent)) {
			cache(tapdataEvent, preTableName);
		}
		MergeInfo mergeInfo = new MergeInfo();
		MergeTableProperties currentMergeTableProperty = this.mergeTablePropertiesMap.get(preNodeId);
		io.tapdata.pdk.apis.entity.merge.MergeTableProperties pdkMergeTableProperties = copyMergeTableProperty(currentMergeTableProperty);
		mergeInfo.setCurrentProperty(pdkMergeTableProperties);
		if (needLookup(tapdataEvent)) {
			List<MergeLookupResult> mergeLookupResults = lookup(tapdataEvent);
			mergeInfo.setMergeLookupResults(mergeLookupResults);
		}
		tapEvent.addInfo(MergeInfo.EVENT_INFO_KEY, mergeInfo);
		consumer.accept(tapdataEvent, ProcessResult.create().tableId(preTableName));
	}

	private void initMergeTableProperties(List<MergeTableProperties> mergeTableProperties) {
		if (null == mergeTableProperties) {
			this.mergeTablePropertiesMap = new HashMap<>();
			Node<?> node = processorBaseContext.getNode();
			if (node instanceof MergeTableNode) {
				mergeTableProperties = ((MergeTableNode) node).getMergeProperties();
			} else {
				throw new RuntimeException("Merge processor node have wrong node type: " + node.getClass().getName());
			}
		}
		if (CollectionUtils.isEmpty(mergeTableProperties)) return;
		for (MergeTableProperties mergeTableProperty : mergeTableProperties) {
			String id = mergeTableProperty.getId();
			this.mergeTablePropertiesMap.put(id, mergeTableProperty);
			List<MergeTableProperties> children = mergeTableProperty.getChildren();
			if (CollectionUtils.isNotEmpty(children)) {
				initMergeTableProperties(children);
			}
		}
	}

	private void initMergeCache() {
		this.mergeCacheMap = new HashMap<>();
		if (MapUtils.isEmpty(this.lookupMap)) {
			return;
		}
		for (List<MergeTableProperties> lookupList : this.lookupMap.values()) {
			for (MergeTableProperties mergeProperty : lookupList) {
				String cacheName = getCacheName(mergeProperty.getId(), mergeProperty.getTableName());
				if (StringUtils.isBlank(cacheName)) {
					break;
				}
				ConstructIMap<Document> hazelcastConstruct = new ConstructIMap<>(jetContext.hazelcastInstance(), cacheName, externalStorageDto);
				this.mergeCacheMap.put(mergeProperty.getId(), hazelcastConstruct);
				logger.info("Create imap name: {}, external storage: {}", cacheName, externalStorageDto);
				obsLogger.info("Create imap name: {}, external storage: {}", cacheName, externalStorageDto);
			}
		}
	}

	private void initLookupMergeProperties() {
		Node<?> node = this.processorBaseContext.getNode();
		this.lookupMap = new HashMap<>();
		this.needCacheIdList = new ArrayList<>();
		List<MergeTableProperties> mergeProperties = ((MergeTableNode) node).getMergeProperties();
		for (MergeTableProperties mergeProperty : mergeProperties) {
			recursiveGetLookupList(mergeProperty);
		}
	}

	private void recursiveGetLookupList(MergeTableProperties mergeTableProperties) {
		List<MergeTableProperties> lookupList = new ArrayList<>();
		List<MergeTableProperties> children = mergeTableProperties.getChildren();
		if (CollectionUtils.isEmpty(children)) return;
		for (MergeTableProperties child : children) {
			lookupList.add(child);
			recursiveGetLookupList(child);
		}
		this.lookupMap.put(mergeTableProperties.getId(), lookupList);
		this.needCacheIdList.addAll(lookupList.stream().map(MergeTableProperties::getId).collect(Collectors.toList()));
		StringBuilder lookupLog = new StringBuilder("\nMerge lookup relation{\n  " + mergeTableProperties.getTableName() + "(" + mergeTableProperties.getId() + ")");
		lookupList.forEach(l -> lookupLog.append("\n    ->").append(l.getTableName()).append("(").append(l.getId()).append(")"));
		lookupLog.append("\n}");
		logger.info(lookupLog);
	}

	private void initSourceNodeMap(List<MergeTableProperties> mergeTableProperties) {
		if (null == mergeTableProperties) {
			this.sourceNodeMap = new HashMap<>();
			Node<?> node = this.processorBaseContext.getNode();
			mergeTableProperties = ((MergeTableNode) node).getMergeProperties();
		}
		if (CollectionUtils.isEmpty(mergeTableProperties)) return;
		for (MergeTableProperties mergeProperty : mergeTableProperties) {
			Node<?> sourceTableNode = getSourceTableNode(mergeProperty.getId());
			if (!(sourceTableNode instanceof TableNode)) {
				throw new RuntimeException(sourceTableNode.getName() + "(" + sourceTableNode.getId() + ", " + sourceTableNode.getClass().getSimpleName() + ") cannot linked to a merge table node");
			}
			this.sourceNodeMap.put(mergeProperty.getId(), sourceTableNode);
			initSourceNodeMap(mergeProperty.getChildren());
		}
	}

	private void initSourceConnectionMap(List<MergeTableProperties> mergeTableProperties) {
		if (null == mergeTableProperties) {
			this.sourceConnectionMap = new HashMap<>();
			Node<?> node = this.processorBaseContext.getNode();
			mergeTableProperties = ((MergeTableNode) node).getMergeProperties();
		}
		if (CollectionUtils.isEmpty(mergeTableProperties)) return;
		for (MergeTableProperties mergeProperty : mergeTableProperties) {
			Node<?> sourceNode = this.sourceNodeMap.get(mergeProperty.getId());
			String connectionId = getConnectionId(sourceNode);
			Query query = new Query(Criteria.where("_id").is(connectionId));
			query.fields().exclude("schema");
			Connections connections = clientMongoOperator.findOne(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
			this.sourceConnectionMap.put(mergeProperty.getId(), connections);
			initSourceConnectionMap(mergeProperty.getChildren());
		}
	}

	private void initSourcePkOrUniqueFieldMap(List<MergeTableProperties> mergeTableProperties) {
		if (null == mergeTableProperties) {
			this.sourcePkOrUniqueFieldMap = new HashMap<>();
			Node<?> node = this.processorBaseContext.getNode();
			mergeTableProperties = ((MergeTableNode) node).getMergeProperties();
		}
		if (CollectionUtils.isEmpty(mergeTableProperties)) return;
		TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
		for (MergeTableProperties mergeProperty : mergeTableProperties) {
			String sourceNodeId = mergeProperty.getId();
			Node<?> preNode = processorBaseContext.getNodes().stream().filter(n -> n.getId().equals(sourceNodeId)).findFirst().orElse(null);
			String tableName = getTableName(preNode);
			TapTable tapTable = tapTableMap.get(tableName);
			MergeTableProperties.MergeType mergeType = mergeProperty.getMergeType();
			List<String> arrayKeys = mergeProperty.getArrayKeys();
			Collection<String> primaryKeys = tapTable.primaryKeys(true);
			List<String> fieldNames;
			switch (mergeType) {
				case appendWrite:
				case updateOrInsert:
				case updateWrite:
					if (CollectionUtils.isEmpty(primaryKeys)) {
						throw new RuntimeException("Source table " + tableName + " not have unique keys, cannot do merge table operation: " + mergeType);
					}
					fieldNames = new ArrayList<>(primaryKeys);
					break;
				case updateIntoArray:
					if (CollectionUtils.isEmpty(arrayKeys)) {
						throw new RuntimeException("Source table " + tableName + " not have array keys, cannot do merge table operation: " + mergeType);
					}
					fieldNames = arrayKeys;
					break;
				default:
					throw new RuntimeException("Unrecognized merge type: " + mergeType);
			}
			this.sourcePkOrUniqueFieldMap.put(sourceNodeId, fieldNames);
			initSourcePkOrUniqueFieldMap(mergeProperty.getChildren());
		}
	}

	private static String getCacheName(String nodeId, String tableName) {
		String name;
//    if (StringUtils.isBlank(tableName)) {
//      throw new RuntimeException("Get merge node cache name failed, table name is blank");
//    }
		if (StringUtils.isBlank(nodeId)) {
			throw new RuntimeException("Get merge node cache name failed, node id is blank");
		}
		if (StringUtils.isBlank(tableName)) {
			name = TAG + "_" + nodeId + "_" + LOOKUP_TABLE_SUFFIX;
		} else {
			name = TAG + "_" + tableName + "_" + nodeId + "_" + LOOKUP_TABLE_SUFFIX;
		}
		return name;
	}

	private MergeTableProperties getMergeProperty(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) {
			return null;
		}
		String preNodeId = getPreNodeId(tapdataEvent);
		return this.mergeTablePropertiesMap.get(preNodeId);
	}

	private MergeTableProperties.MergeType getMergeType(TapdataEvent tapdataEvent) {
		MergeTableProperties mergeProperty = getMergeProperty(tapdataEvent);
		if (null == mergeProperty) {
			throw new RuntimeException("Cannot found merge property by node id: " + getMergeProperty(tapdataEvent));
		}
		return mergeProperty.getMergeType();
	}

	private ConstructIMap<Document> getHazelcastConstruct(String sourceNodeId) {
		ConstructIMap<Document> hazelcastConstruct = this.mergeCacheMap.getOrDefault(sourceNodeId, null);
		if (null == hazelcastConstruct) {
			throw new HazelcastNotExistsException("Cannot found hazelcast cache by node id: " + sourceNodeId);
		}
		return hazelcastConstruct;
	}

	private String getPreNodeId(TapdataEvent tapdataEvent) {
		List<String> nodeIds = tapdataEvent.getNodeIds();
		if (CollectionUtils.isEmpty(nodeIds)) {
			throw new RuntimeException("From node id list is empty");
		}
		return nodeIds.get(nodeIds.size() - 1);
	}

	private boolean needCache(TapdataEvent tapdataEvent) {
		if (isInvalidOperation(tapdataEvent)) return false;
		String preNodeId = getPreNodeId(tapdataEvent);
		return needCacheIdList.contains(preNodeId);
	}

	private boolean needLookup(TapdataEvent tapdataEvent) {
		if (isInvalidOperation(tapdataEvent)) return false;
		String op = getOp(tapdataEvent);
		if (op.equals(OperationType.DELETE.getOp())) {
			return false;
		}
		String preNodeId = getPreNodeId(tapdataEvent);
		return this.lookupMap.containsKey(preNodeId);
	}

	private String getOp(TapdataEvent tapdataEvent) {
		String op = "";
		if (tapdataEvent.getTapEvent() instanceof TapRecordEvent) {
			op = TapEventUtil.getOp(tapdataEvent.getTapEvent());
		} else if (null != tapdataEvent.getMessageEntity()) {
			op = tapdataEvent.getMessageEntity().getOp();
		}
		return op;
	}

	private boolean isInvalidOperation(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) {
			return true;
		}
		String op = getOp(tapdataEvent);
		if (StringUtils.isBlank(op)) {
			return true;
		}
		return !OperationType.isDml(op);
	}

	private void cache(TapdataEvent tapdataEvent, String preTableName) {
		String op = getOp(tapdataEvent);
		OperationType operationType = OperationType.fromOp(op);
		ConstructIMap<Document> hazelcastConstruct = getHazelcastConstruct(getPreNodeId(tapdataEvent));
		MergeTableProperties mergeProperty = getMergeProperty(tapdataEvent);
		switch (operationType) {
			case INSERT:
			case UPDATE:
				try {
					upsertCache(tapdataEvent, mergeProperty, hazelcastConstruct);
				} catch (Exception e) {
					throw new RuntimeException("tableName: " + preTableName + ";\n " + e.getMessage() + ";\nError: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
				}
				break;
			case DELETE:
				try {
					deleteCache(tapdataEvent, mergeProperty, hazelcastConstruct);
				} catch (Exception e) {
					throw new RuntimeException("tableName: " + preTableName + ";\n " + e.getMessage() + ";\nError: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
				}
				break;
			default:
				break;
		}
	}

	private void upsertCache(TapdataEvent tapdataEvent, MergeTableProperties mergeTableProperty, ConstructIMap<Document> hazelcastConstruct) throws Exception {
		Map<String, Object> after = getAfter(tapdataEvent);
		String joinValueKey = getJoinValueKeyBySource(after, mergeTableProperty);
		String pkOrUniqueKey = getPkOrUniqueValueKey(after, mergeTableProperty);
		Document groupByJoinKeyValues;
		try {
			groupByJoinKeyValues = hazelcastConstruct.find(joinValueKey);
		} catch (Exception e) {
			throw new Exception("Find value by join key value string(" + joinValueKey + ") error", e);
		}
		if (null == groupByJoinKeyValues) {
			groupByJoinKeyValues = new Document();
		}
		groupByJoinKeyValues.put(pkOrUniqueKey, after);
		try {
			hazelcastConstruct.upsert(joinValueKey, groupByJoinKeyValues);
		} catch (Exception e) {
			throw new Exception("Upsert value error, join value key: " + joinValueKey + ", data: " + groupByJoinKeyValues, e);
		}
	}

	private void deleteCache(TapdataEvent tapdataEvent, MergeTableProperties mergeTableProperty, ConstructIMap<Document> hazelcastConstruct) throws Exception {
		Map<String, Object> before = getBefore(tapdataEvent);
		String joinValueKey = getJoinValueKeyBySource(before, mergeTableProperty);
		String pkOrUniqueValueKey = getPkOrUniqueValueKey(before, mergeTableProperty);
		Document groupByJoinKeyValues;
		try {
			groupByJoinKeyValues = hazelcastConstruct.find(joinValueKey);
		} catch (Exception e) {
			throw new Exception("Find value by join key value string(" + joinValueKey + ") error", e);
		}
		if (null == groupByJoinKeyValues) {
			return;
		}
		groupByJoinKeyValues.remove(pkOrUniqueValueKey);
		if (MapUtils.isEmpty(groupByJoinKeyValues)) {
			try {
				hazelcastConstruct.delete(joinValueKey);
			} catch (Exception e) {
				throw new Exception("Remove value error, join value key: " + joinValueKey, e);
			}
		}
	}

	private Map<String, Object> getAfter(TapdataEvent tapdataEvent) {
		Map<String, Object> after = null;
		if (tapdataEvent.getTapEvent() instanceof TapRecordEvent) {
			after = TapEventUtil.getAfter(tapdataEvent.getTapEvent());
		} else if (null != tapdataEvent.getMessageEntity()) {
			after = tapdataEvent.getMessageEntity().getAfter();
		}
		return after;
	}

	private Map<String, Object> getBefore(TapdataEvent tapdataEvent) {
		Map<String, Object> before = null;
		if (tapdataEvent.getTapEvent() instanceof TapRecordEvent) {
			before = TapEventUtil.getBefore(tapdataEvent.getTapEvent());
		} else if (null != tapdataEvent.getMessageEntity()) {
			before = tapdataEvent.getMessageEntity().getBefore();
		}
		return before;
	}

	private String getJoinValueKeyBySource(Map<String, Object> data, MergeTableProperties mergeProperty) {
		List<Map<String, String>> joinKeys = mergeProperty.getJoinKeys();
		List<String> joinKeyList;
		joinKeyList = joinKeys.stream().map(j -> j.get(JoinConditionType.SOURCE.getType())).collect(Collectors.toList());
		if (CollectionUtils.isEmpty(joinKeyList)) {
			throw new RuntimeException("Join key is empty: " + mergeProperty);
		}
		if (MapUtils.isEmpty(data)) {
			return "";
		}
		List<String> values = new ArrayList<>();
		for (String joinKey : joinKeyList) {
			Object value = MapUtilV2.getValueByKey(data, joinKey);
			if (value instanceof NotExistsNode) {
				throw new RuntimeException("Cannot found value in data by join key: " + joinKey + ", data: " + data);
			}
			values.add(String.valueOf(value));
		}
		return Base64.getEncoder().encodeToString(String.join("_", values).getBytes(StandardCharsets.UTF_8));
	}

	private String getJoinValueKeyByTarget(Map<String, Object> data, MergeTableProperties mergeProperty, MergeTableProperties lastMergeProperty) {
		List<Map<String, String>> joinKeys = mergeProperty.getJoinKeys();
		List<String> joinKeyList;
		try {
			joinKeyList = getJoinKeys(joinKeys, JoinConditionType.TARGET);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage() + ": " + mergeProperty);
		}
		if (MapUtils.isEmpty(data)) {
			return "";
		}
		List<String> values = new ArrayList<>();
		for (String joinKey : joinKeyList) {
			if (null != lastMergeProperty && StringUtils.isNotBlank(lastMergeProperty.getTargetPath())) {
				joinKey = StringUtils.removeStart(joinKey, lastMergeProperty.getTargetPath() + ".");
			}
			Object value = MapUtilV2.getValueByKey(data, joinKey);
			if (value instanceof NotExistsNode) {
//				throw new RuntimeException("Cannot found value in data by join key: " + joinKey + ", data: " + data);
				return null;
			}
			values.add(String.valueOf(value));
		}
		return Base64.getEncoder().encodeToString(String.join("_", values).getBytes(StandardCharsets.UTF_8));
	}

	private List<String> getJoinKeys(List<Map<String, String>> joinKeys, JoinConditionType joinConditionType) {
		List<String> result;
		result = joinKeys.stream().map(j -> j.get(joinConditionType.getType())).collect(Collectors.toList());
		if (CollectionUtils.isEmpty(result)) throw new RuntimeException("Join key is empty");
		return result;
	}

	private String getPkOrUniqueValueKey(Map<String, Object> data, MergeTableProperties mergeProperty) {
		String sourceNodeId = mergeProperty.getId();
		List<String> pkOrUniqueFields = this.sourcePkOrUniqueFieldMap.get(sourceNodeId);
		if (MapUtils.isEmpty(data)) {
			return "";
		}
		List<String> values = new ArrayList<>();
		for (String pkOrUniqueField : pkOrUniqueFields) {
			Object value = MapUtilV2.getValueByKey(data, pkOrUniqueField);
			if (value instanceof NotExistsNode) {
				throw new RuntimeException("Cannot found value in data by pk or unique field name: " + pkOrUniqueField + ", data: " + data);
			}
			values.add(String.valueOf(value));
		}
		return String.join("_", values);
	}

	private Map<String, RelateDatabaseField> getSourceFieldMap(String connectionId, String sourceTableName) {
		Query query = new Query(Criteria.where("_id").is(connectionId));
		query.fields().exclude("schema");
		Connections connections = this.clientMongoOperator.findOne(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
		List<RelateDataBaseTable> tables = connections.getSchema().get("tables");
		return ((SchemaList<String, RelateDataBaseTable>) tables).getFieldMap(sourceTableName);
	}

	private Node<?> getSourceTableNode(String sourceId) {
		Node<?> node = this.processorBaseContext.getNode();
		List<? extends Node<?>> predecessors = node.predecessors();
		predecessors = predecessors.stream().filter(n -> n.getId().equals(sourceId)).collect(Collectors.toList());
		predecessors = GraphUtil.predecessors(node, Node::isDataNode, (List<Node<?>>) predecessors);
		if (CollectionUtils.isEmpty(predecessors)) {
			throw new RuntimeException("Cannot found pre node by merge table node: " + node.getName() + "(" + node.getId() + "), source id: " + sourceId);
		}
		return predecessors.get(0);
	}

	private String getTableName(Node<?> preTableNode) {
		String tableName;
		if (preTableNode instanceof TableNode) {
			tableName = ((TableNode) preTableNode).getTableName();
			if (StringUtils.isBlank(tableName)) {
				throw new RuntimeException("Table node " + preTableNode.getName() + "(" + preTableNode.getId() + ")'s table name cannot be blank");
			}
		} else {
			tableName = preTableNode.getId();
		}
		return tableName;
	}

	private String getConnectionId(Node<?> preTableNode) {
		String connectionId;
		if (preTableNode instanceof TableNode) {
			connectionId = ((TableNode) preTableNode).getConnectionId();
			if (StringUtils.isBlank(connectionId)) {
				throw new RuntimeException("Table node " + preTableNode.getName() + "(" + preTableNode.getId() + ")'s connection id cannot be blank");
			}
		} else {
			throw new RuntimeException(preTableNode.getName() + "(" + preTableNode.getId() + ", " + preTableNode.getClass().getSimpleName() + ") cannot linked to a merge table node");
		}
		return connectionId;
	}

	private List<MergeLookupResult> lookup(TapdataEvent tapdataEvent) {
		List<String> nodeIds = tapdataEvent.getNodeIds();
		if (CollectionUtils.isEmpty(nodeIds)) {
			throw new RuntimeException("Merge table node lookup failed, from node id list is empty");
		}
		String sourceNodeId = getPreNodeId(tapdataEvent);
		MergeTableProperties currentMergeTableProperty = this.mergeTablePropertiesMap.get(sourceNodeId);
		Map<String, Object> after = getAfter(tapdataEvent);
		return recursiveLookup(currentMergeTableProperty, after);
	}

	private List<MergeLookupResult> recursiveLookup(MergeTableProperties mergeTableProperties,
													Map<String, Object> data) {
		List<MergeTableProperties> children = mergeTableProperties.getChildren();
		if (CollectionUtils.isEmpty(children)) return null;
		List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
		for (MergeTableProperties childMergeProperty : children) {
			MergeTableProperties.MergeType mergeType = childMergeProperty.getMergeType();
			ConstructIMap<Document> hazelcastConstruct = getHazelcastConstruct(childMergeProperty.getId());
			String joinValueKey = getJoinValueKeyByTarget(data, childMergeProperty, mergeTableProperties);
			if (joinValueKey == null) {
				continue;
			}
			Document findData;
			try {
				findData = hazelcastConstruct.find(joinValueKey);
			} catch (Exception e) {
				throw new RuntimeException("Merge table node lookup in cache failed, join values key" + joinValueKey + ", data: " + data);
			}
			if (MapUtils.isEmpty(findData)) {
				return mergeLookupResults;
			}
			io.tapdata.pdk.apis.entity.merge.MergeTableProperties pdkMergeTableProperty = copyMergeTableProperty(childMergeProperty);
			if (MergeTableProperties.MergeType.updateWrite == mergeType) {
				Set<String> keySet = findData.keySet();
				keySet.remove("_ts");
				if (keySet.size() > 1) {
					logger.warn("Update write merge lookup, find more than one row by join key: " + joinValueKey + ", will use first row: " + data);
				}
				String firstKey = findData.keySet().iterator().next();
				Map<String, Object> lookupMap = (Map<String, Object>) findData.get(firstKey);
				MergeLookupResult mergeLookupResult = new MergeLookupResult();
				mergeLookupResult.setProperty(pdkMergeTableProperty);
				mergeLookupResult.setData(lookupMap);
				mergeLookupResult.setMergeLookupResults(recursiveLookup(childMergeProperty, lookupMap));
				mergeLookupResults.add(mergeLookupResult);
			} else if (MergeTableProperties.MergeType.updateIntoArray == mergeType) {
				Collection<Object> lookupArray = findData.values();
				for (Object arrayData : lookupArray) {
					if (!(arrayData instanceof Map)) continue;
					MergeLookupResult mergeLookupResult = new MergeLookupResult();
					mergeLookupResult.setProperty(pdkMergeTableProperty);
					mergeLookupResult.setData((Map<String, Object>) arrayData);
					mergeLookupResult.setMergeLookupResults(recursiveLookup(childMergeProperty, (Map<String, Object>) arrayData));
					mergeLookupResults.add(mergeLookupResult);
				}
			}
		}
		return mergeLookupResults;
	}

	private enum JoinConditionType {
		SOURCE("source"), TARGET("target"),
		;

		private final String type;

		JoinConditionType(String type) {
			this.type = type;
		}

		public String getType() {
			return type;
		}
	}

	private io.tapdata.pdk.apis.entity.merge.MergeTableProperties copyMergeTableProperty(MergeTableProperties mergeTableProperties) {
		if (null == mergeTableProperties) return null;
		io.tapdata.pdk.apis.entity.merge.MergeTableProperties pdkMergeTableProperties = new io.tapdata.pdk.apis.entity.merge.MergeTableProperties();
		BeanUtils.copyProperties(mergeTableProperties, pdkMergeTableProperties);
		pdkMergeTableProperties.setIsArray(mergeTableProperties.getIsArray());
		if (mergeTableProperties.getMergeType() != null) {
			pdkMergeTableProperties.setMergeType(io.tapdata.pdk.apis.entity.merge.MergeTableProperties.MergeType.valueOf(mergeTableProperties.getMergeType().name()));
		}
		return pdkMergeTableProperties;
	}

	public static void clearCache(Node<?> node) {
		if (!(node instanceof MergeTableNode)) return;
		ExternalStorageDto externalStorage = ExternalStorageUtil.getExternalStorage(node);
		recursiveClearCache(externalStorage, ((MergeTableNode) node).getMergeProperties(), HazelcastUtil.getInstance());
	}

	private static void recursiveClearCache(ExternalStorageDto externalStorageDto, List<MergeTableProperties> mergeTableProperties, HazelcastInstance hazelcastInstance) {
		if (CollectionUtils.isEmpty(mergeTableProperties)) return;
		for (MergeTableProperties mergeTableProperty : mergeTableProperties) {
			String cacheName = getCacheName(mergeTableProperty.getId(), mergeTableProperty.getTableName());
			ConstructIMap<Document> imap = new ConstructIMap<>(hazelcastInstance, cacheName, externalStorageDto);
			try {
				imap.clear();
				imap.destroy();
			} catch (Exception e) {
				throw new RuntimeException("Clear imap failed, name: " + cacheName + ", error message: " + e.getMessage(), e);
			}
			recursiveClearCache(externalStorageDto, mergeTableProperty.getChildren(), hazelcastInstance);
		}
	}

	private TapCreateIndexEvent generateCreateIndexEventsForTarget(){
		if (MapUtils.isNotEmpty(mergeTablePropertiesMap)) {
			List<TapIndex> indexList = new ArrayList<>(mergeTablePropertiesMap.size());
			for (MergeTableProperties mergeTableProperties : mergeTablePropertiesMap.values()) {
				final List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
				if (CollectionUtils.isNotEmpty(joinKeys)) {
					TapIndex tapIndex = new TapIndex();
					for (Map<String, String> joinKey : joinKeys) {
						if (MapUtils.isNotEmpty(joinKey)) {
							tapIndex.indexField(new TapIndexField().name(joinKey.get("target")).fieldAsc(true));
						}
					}
					if (CollectionUtils.isNotEmpty(tapIndex.getIndexFields())) {
						indexList.add(tapIndex);
					}
				}

				if (CollectionUtils.isNotEmpty(mergeTableProperties.getArrayKeys())) {
					TapIndex tapIndex = new TapIndex();
					for (String arrayKey : mergeTableProperties.getArrayKeys()) {
						StringBuilder sb = new StringBuilder();
						if (StringUtils.isNotBlank(mergeTableProperties.getTargetPath())) {
							sb.append(mergeTableProperties.getTargetPath()).append(".");
						}
						sb.append(arrayKey);
						tapIndex.indexField(new TapIndexField().name(sb.toString()).fieldAsc(true));
					}

					if (CollectionUtils.isNotEmpty(tapIndex.getIndexFields())) {
						indexList.add(tapIndex);
					}
				}
			}
			if (CollectionUtils.isNotEmpty(indexList)) {
				final TapCreateIndexEvent tapCreateIndexEvent = new TapCreateIndexEvent();
				tapCreateIndexEvent.setTableId(this.getNode().getId());
				return tapCreateIndexEvent.indexList(indexList);
			}
		}
		return null;
	}

	@Override
	protected void doClose() throws Exception {
		if (MapUtils.isNotEmpty(mergeCacheMap)) {
			for (ConstructIMap<Document> constructIMap : mergeCacheMap.values()) {
				PersistenceStorage.getInstance().destroy(constructIMap.getName());
			}
		}
		super.doClose();
	}
}

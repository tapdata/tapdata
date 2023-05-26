package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.MapUtilV2;
import com.tapdata.constant.NotExistsNode;
import com.tapdata.entity.Connections;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
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
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TaskMergeProcessorExCode_16;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.core.utils.CommonUtils;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
	private final Map<String, Node<?>> preNodeMap = new ConcurrentHashMap<>();

	public HazelcastMergeNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}
	private void selfCheckNode(Node node) {
		if (node instanceof MergeTableNode) {
			MergeTableNode mergeTableNode = (MergeTableNode) node;
			selfCheckMergeTableProperties(mergeTableNode.getMergeProperties());
		}
	}

	private void selfCheckMergeTableProperties(List<MergeTableProperties> mergeTableProperties) {
		if(mergeTableProperties == null)
			return;
		for(MergeTableProperties tableProperties : mergeTableProperties) {
			if(tableProperties.getMergeType().equals(MergeTableProperties.MergeType.updateIntoArray) || tableProperties.getIsArray()) {
				boolean intoArray = tableProperties.getMergeType().equals(MergeTableProperties.MergeType.updateIntoArray);
				List<MergeTableProperties> children = tableProperties.getChildren();
				if(children != null) {
					for (MergeTableProperties ch : children) {
						if(!ch.getIsArray()) {
							ch.setArray(true);

//							TapLogger.warn(TAG, "Fixed merge table properties, set array to true when mergeType is updateIntoArray, table: " + ch.getTableName() + " targetPath: " + ch.getTargetPath());
						}
						if(ch.getArrayPath() == null) {
							ch.setArrayPath(intoArray ? tableProperties.getTargetPath() : tableProperties.getArrayPath());
						}
					}
				}
			}

			selfCheckMergeTableProperties(tableProperties.getChildren());
		}
	}
	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		selfCheckNode(getNode());

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
		Node<?> preNode;
		try {
			preNode = getPreNode(preNodeId);
		} catch (Exception e) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.CANNOT_FOUND_PRE_NODE, "Event from node list: " + tapdataEvent.getNodeIds(), e);
		}
		String preTableName;
		if (preNode instanceof TableNode) {
			preTableName = ((TableNode) preNode).getTableName();
		} else {
			preTableName = preNodeId;
		}
		if (needCache(tapdataEvent)) {
			cache(tapdataEvent);
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

	@NotNull
	private Node<?> getPreNode(String preNodeId) {
		return preNodeMap.computeIfAbsent(preNodeId, k -> {
			Node<?> foundNode = processorBaseContext.getNodes().stream().filter(n -> n.getId().equals(preNodeId)).findFirst().orElse(null);
			if (null == foundNode)
				throw new RuntimeException("Pre node not exists in task dag node list, node id: " + preNodeId);
			return foundNode;
		});
	}

	private void initMergeTableProperties(List<MergeTableProperties> mergeTableProperties) {
		if (null == mergeTableProperties) {
			this.mergeTablePropertiesMap = new HashMap<>();
			Node<?> node = processorBaseContext.getNode();
			if (node instanceof MergeTableNode) {
				mergeTableProperties = ((MergeTableNode) node).getMergeProperties();
			} else {
				throw new TapCodeException(TaskMergeProcessorExCode_16.WRONG_NODE_TYPE, "Expect MergeTableNode, but got: " + node.getClass().getSimpleName());
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
				String cacheName;
				try {
					cacheName = getCacheName(mergeProperty.getId(), mergeProperty.getTableName());
				} catch (Exception e) {
					throw new TapCodeException(TaskMergeProcessorExCode_16.INIT_MERGE_CACHE_GET_CACHE_NAME_FAILED, e);
				}
				if (StringUtils.isBlank(cacheName)) {
					break;
				}
				ConstructIMap<Document> hazelcastConstruct = new ConstructIMap<>(jetContext.hazelcastInstance(), HazelcastMergeNode.class.getSimpleName(), cacheName, externalStorageDto);
				this.mergeCacheMap.put(mergeProperty.getId(), hazelcastConstruct);
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
				throw new TapCodeException(TaskMergeProcessorExCode_16.INIT_SOURCE_NODE_MAP_WRONG_NODE_TYPE, "Expect TableNode, but got: " + sourceTableNode.getClass().getSimpleName());
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
			if (null == preNode) {
				throw new TapCodeException(TaskMergeProcessorExCode_16.TAP_MERGE_TABLE_NODE_NOT_FOUND, String.format("- Node ID: %s", sourceNodeId));
			}
			String nodeName = preNode.getName();
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
						throw new TapCodeException(TaskMergeProcessorExCode_16.TAP_MERGE_TABLE_NO_PRIMARY_KEY, String.format("- Table name: %s\n- Node name: %s\n- Merge operation: %s", tableName, nodeName, mergeType));
					}
					fieldNames = new ArrayList<>(primaryKeys);
					break;
				case updateIntoArray:
					if (CollectionUtils.isEmpty(arrayKeys)) {
						throw new TapCodeException(TaskMergeProcessorExCode_16.TAP_MERGE_TABLE_NO_ARRAY_KEY, String.format("- Table name: %s- Node name: %s\n", tableName, nodeName));
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

	private ConstructIMap<Document> getHazelcastConstruct(String sourceNodeId) {
		ConstructIMap<Document> hazelcastConstruct = this.mergeCacheMap.getOrDefault(sourceNodeId, null);
		if (null == hazelcastConstruct) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.NOT_FOUND_CACHE_IN_MEMORY_MAP, String.format("Find cache by node id: %s\nMerge memory map key-value: %s",
					sourceNodeId, this.mergeCacheMap.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(","))));
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
		SyncStage syncStage = tapdataEvent.getSyncStage();
		if (SyncStage.INITIAL_SYNC.equals(syncStage)) {
			return false;
		}
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

	private void cache(TapdataEvent tapdataEvent) {
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
					if (e instanceof TapCodeException) {
						throw (TapCodeException) e;
					} else {
						throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHE_UNKNOWN_ERROR, e);
					}
				}
				break;
			case DELETE:
				try {
					deleteCache(tapdataEvent, mergeProperty, hazelcastConstruct);
				} catch (Exception e) {
					if (e instanceof TapCodeException) {
						throw (TapCodeException) e;
					} else {
						throw new TapCodeException(TaskMergeProcessorExCode_16.DELETE_CACHE_UNKNOWN_ERROR, e);
					}
				}
				break;
			default:
				break;
		}
	}

	private void upsertCache(TapdataEvent tapdataEvent, MergeTableProperties mergeTableProperty, ConstructIMap<Document> hazelcastConstruct) throws Exception {
		Map<String, Object> after = getAfter(tapdataEvent);
		String joinValueKey = getJoinValueKeyBySource(after, mergeTableProperty);
		String encodeJoinValueKey = encode(joinValueKey);
		String pkOrUniqueValueKey = getPkOrUniqueValueKey(after, mergeTableProperty);
		String encodePkOrUniqueValueKey = encode(pkOrUniqueValueKey);
		Document groupByJoinKeyValues;
		try {
			groupByJoinKeyValues = hazelcastConstruct.find(encodeJoinValueKey);
		} catch (Exception e) {
			throw new TapEventException(TaskMergeProcessorExCode_16.UPSERT_CACHE_FIND_BY_JOIN_KEY_FAILED, String.format("- Construct name: %s\n- Join value key: %s, encode: %s",
					hazelcastConstruct.getName(), joinValueKey, encodeJoinValueKey), e).addEvent(tapdataEvent.getTapEvent());
		}
		if (null == groupByJoinKeyValues) {
			groupByJoinKeyValues = new Document();
		}
		groupByJoinKeyValues.put(encodePkOrUniqueValueKey, after);
		try {
			hazelcastConstruct.upsert(encodeJoinValueKey, groupByJoinKeyValues);
		} catch (Exception e) {
			throw new TapEventException(TaskMergeProcessorExCode_16.UPSERT_CACHE_FAILED, String.format("- Construct name: %s\n- Join value key: %s, encode: %s\n- Pk or unique values: %s, encode: %s\n- Find by join value key result: %s",
					hazelcastConstruct.getName(), joinValueKey, encodeJoinValueKey, pkOrUniqueValueKey, encodePkOrUniqueValueKey, groupByJoinKeyValues.toJson()), e).addEvent(tapdataEvent.getTapEvent());
		}
	}

	private void deleteCache(TapdataEvent tapdataEvent, MergeTableProperties mergeTableProperty, ConstructIMap<Document> hazelcastConstruct) throws Exception {
		Map<String, Object> before = getBefore(tapdataEvent);
		String joinValueKey = getJoinValueKeyBySource(before, mergeTableProperty);
		String encodeJoinValueKey = encode(joinValueKey);
		String pkOrUniqueValueKey = getPkOrUniqueValueKey(before, mergeTableProperty);
		String encodePkOrUniqueValueKey = encode(pkOrUniqueValueKey);
		Document groupByJoinKeyValues;
		try {
			groupByJoinKeyValues = hazelcastConstruct.find(encodeJoinValueKey);
		} catch (Exception e) {
			throw new TapEventException(TaskMergeProcessorExCode_16.DELETE_CACHE_FIND_BY_JOIN_KEY_FAILED, String.format("- Construct name: %s\n- Join value key: %s, encode: %s",
					hazelcastConstruct.getName(), joinValueKey, encodeJoinValueKey), e).addEvent(tapdataEvent.getTapEvent());
		}
		if (null == groupByJoinKeyValues) {
			return;
		}
		groupByJoinKeyValues.remove(encodePkOrUniqueValueKey);
		if (MapUtils.isEmpty(groupByJoinKeyValues)) {
			try {
				hazelcastConstruct.delete(encodeJoinValueKey);
			} catch (Exception e) {
				throw new TapEventException(TaskMergeProcessorExCode_16.DELETE_CACHE_FAILED, String.format("- Construct name: %s\n- Join value key: %s, encode: %s\n- Pk or unique value key: %s, encode: %s\n- Find by join value key result: %s",
						hazelcastConstruct.getName(), joinValueKey, encodeJoinValueKey, pkOrUniqueValueKey, encodePkOrUniqueValueKey, groupByJoinKeyValues.toJson()), e).addEvent(tapdataEvent.getTapEvent());
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
		List<String> joinKeyList = getJoinKeys(joinKeys, JoinConditionType.SOURCE);
		if (CollectionUtils.isEmpty(joinKeyList)) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.MISSING_SOURCE_JOIN_KEY_CONFIG, String.format("Merge property: %s", mergeProperty));
		}
		if (MapUtils.isEmpty(data)) {
			return "";
		}
		List<String> values = new ArrayList<>();
		for (String joinKey : joinKeyList) {
			Object value = MapUtilV2.getValueByKey(data, joinKey);
			if (value instanceof NotExistsNode) {
				throw new TapCodeException(TaskMergeProcessorExCode_16.JOIN_KEY_VALUE_NOT_EXISTS, String.format("- Join key: %s\n- Data: %s", joinKey, data));
			}
			values.add(String.valueOf(value));
		}
		return String.join("_", values);
	}

	private String getJoinValueKeyByTarget(Map<String, Object> data, MergeTableProperties mergeProperty, MergeTableProperties lastMergeProperty) {
		List<Map<String, String>> joinKeys = mergeProperty.getJoinKeys();
		List<String> joinKeyList = getJoinKeys(joinKeys, JoinConditionType.TARGET);
		if (CollectionUtils.isEmpty(joinKeyList)) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.MISSING_TARGET_JOIN_KEY_CONFIG, String.format("Merge property: %s", mergeProperty));
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
				return null;
			}
			values.add(String.valueOf(value));
		}
		return String.join("_", values);
	}

	private String encode(String str) {
		return Base64.getEncoder().encodeToString(str.getBytes(StandardCharsets.UTF_8));
	}

	private List<String> getJoinKeys(List<Map<String, String>> joinKeys, JoinConditionType joinConditionType) {
		if (null == joinKeys) {
			return Collections.emptyList();
		}
		return joinKeys.stream().map(j -> j.get(joinConditionType.getType())).collect(Collectors.toList());
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
				throw new TapCodeException(TaskMergeProcessorExCode_16.PK_OR_UNIQUE_VALUE_NOT_EXISTS, String.format("- Pk or unique field: %s\n- Data: %s", pkOrUniqueField, data));
			}
			values.add(String.valueOf(value));
		}
		return String.join("_", values);
	}

	private Node<?> getSourceTableNode(String sourceId) {
		Node<?> node = this.processorBaseContext.getNode();
		List<? extends Node<?>> predecessors = node.predecessors();
		predecessors = predecessors.stream().filter(n -> n.getId().equals(sourceId)).collect(Collectors.toList());
		predecessors = GraphUtil.predecessors(node, Node::isDataNode, (List<Node<?>>) predecessors);
		if (CollectionUtils.isEmpty(predecessors)) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.CANNOT_FOUND_PRE_NODE, String.format("Source id: %s", sourceId));
		}
		return predecessors.get(0);
	}

	private String getTableName(Node<?> preTableNode) {
		String tableName;
		if (preTableNode instanceof TableNode) {
			tableName = ((TableNode) preTableNode).getTableName();
			if (StringUtils.isBlank(tableName)) {
				throw new TapCodeException(TaskMergeProcessorExCode_16.TABLE_NAME_CANNOT_BE_BLANK, String.format("Table node: %s", preTableNode));
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
				throw new TapCodeException(TaskMergeProcessorExCode_16.CONNECTION_ID_CANNOT_BE_BLANK, String.format("Table node: %s", preTableNode));
			}
		} else {
			throw new RuntimeException(preTableNode.getName() + "(" + preTableNode.getId() + ", " + preTableNode.getClass().getSimpleName() + ") cannot linked to a merge table node");
		}
		return connectionId;
	}

	private List<MergeLookupResult> lookup(TapdataEvent tapdataEvent) {
		List<String> nodeIds = tapdataEvent.getNodeIds();
		if (CollectionUtils.isEmpty(nodeIds)) {
			throw new TapEventException(TaskMergeProcessorExCode_16.LOOK_UP_MISSING_FROM_NODE_ID).addEvent(tapdataEvent.getTapEvent());
		}
		String sourceNodeId = getPreNodeId(tapdataEvent);
		MergeTableProperties currentMergeTableProperty = this.mergeTablePropertiesMap.get(sourceNodeId);
		Map<String, Object> after = getAfter(tapdataEvent);
		List<MergeLookupResult> mergeLookupResults;
		try {
			mergeLookupResults = recursiveLookup(currentMergeTableProperty, after);
		} catch (Exception e) {
			if (e instanceof TapCodeException) {
				throw new TapEventException(((TapCodeException) e).getCode(), e.getMessage(), e.getCause()).addEvent(tapdataEvent.getTapEvent());
			} else {
				throw e;
			}
		}
		return mergeLookupResults;
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
			String encodeJoinValueKey = encode(joinValueKey);
			Document findData;
			try {
				findData = hazelcastConstruct.find(encodeJoinValueKey);
			} catch (Exception e) {
				throw new TapCodeException(TaskMergeProcessorExCode_16.LOOK_UP_FIND_BY_JOIN_KEY_FAILED, String.format("- Find construct name: %s\n- Join key: %s\n- Encoded join key: %s", hazelcastConstruct.getName(), joinValueKey, encodeJoinValueKey), e);
			}
			if (MapUtils.isEmpty(findData)) {
				continue;
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
		CommonUtils.handleAnyErrors((Consumer<Throwable> consumer) -> {
			String cacheName;
			for (MergeTableProperties mergeTableProperty : mergeTableProperties) {
				try {
					try {
						cacheName = getCacheName(mergeTableProperty.getId(), mergeTableProperty.getTableName());
					} catch (Exception e) {
						throw new TapCodeException(TaskMergeProcessorExCode_16.CLEAR_MERGE_CACHE_GET_CACHE_NAME_FAILED, e);
					}
					ConstructIMap<Document> imap = new ConstructIMap<>(hazelcastInstance, HazelcastMergeNode.class.getSimpleName(), cacheName, externalStorageDto);
					try {
						imap.clear();
						imap.destroy();
					} catch (Exception e) {
						throw new RuntimeException("Clear imap failed, name: " + cacheName + ", error message: " + e.getMessage(), e);
					}
					recursiveClearCache(externalStorageDto, mergeTableProperty.getChildren(), hazelcastInstance);
				} catch (Throwable e) {
					consumer.accept(e);
				}
			}
		}, null);
	}

	private TapCreateIndexEvent generateCreateIndexEventsForTarget() {
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
				try {
					constructIMap.destroy();
				} catch (Exception e) {
					logger.warn("Destroy merge cache failed: {}", e.getMessage());
				}
			}
		}
		super.doClose();
	}
}

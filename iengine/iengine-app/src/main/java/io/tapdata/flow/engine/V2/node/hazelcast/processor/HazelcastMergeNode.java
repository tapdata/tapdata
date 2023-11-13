package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.*;
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
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TaskMergeProcessorExCode_16;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import lombok.Getter;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.tapdata.constant.ConnectorConstant.LOOKUP_TABLE_SUFFIX;

/**
 * @author samuel
 * @Description
 * @create 2022-03-23 11:44
 **/
public class HazelcastMergeNode extends HazelcastProcessorBaseNode implements MemoryFetcher {

	public static final String TAG = HazelcastMergeNode.class.getSimpleName();
	public static final int DEFAULT_MERGE_CACHE_IN_MEM_SIZE = 10;
	public static final int DEFAULT_LOOKUP_THREAD_NUM = 8;
	public static final String MERGE_LOOKUP_THREAD_NUM_PROP_KEY = "MERGE_LOOKUP_THREAD_NUM";
	public static final String MERGE_CACHE_IN_MEM_SIZE_PROP_KEY = "MERGE_CACHE_IN_MEM_SIZE";
	public static final int DEFAULT_MERGE_CACHE_BATCH_SIZE = 100;
	public static final String MERGE_CACHE_BATCH_SIZE_PROP_KEY = "MERGE_CACHE_BATCH_SIZE";
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
	private final Map<String, io.tapdata.pdk.apis.entity.merge.MergeTableProperties> preNodeIdPdkMergeTablePropertieMap = new ConcurrentHashMap<>();
	private Map<String, Integer> sourceNodeLevelMap;
	private String mergeMode;
	private ExecutorService lookupThreadPool;
	private int cacheBatchSize;
	private int lookupThreadNum;
	private BlockingQueue<Runnable> lookupQueue;
	private Set<String> firstLevelMergeNodeIds;
	private BatchProcessMetrics batchProcessMetrics;
	private long lastBatchProcessFinishMS;

	public HazelcastMergeNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.cacheBatchSize = CommonUtils.getPropertyInt(MERGE_CACHE_BATCH_SIZE_PROP_KEY, DEFAULT_MERGE_CACHE_BATCH_SIZE);
	}

	private void selfCheckNode(Node node) {
		if (node instanceof MergeTableNode) {
			MergeTableNode mergeTableNode = (MergeTableNode) node;
			selfCheckMergeTableProperties(mergeTableNode.getMergeProperties());
		}
	}

	private void selfCheckMergeTableProperties(List<MergeTableProperties> mergeTableProperties) {
		if (mergeTableProperties == null)
			return;
		for (MergeTableProperties tableProperties : mergeTableProperties) {
			if (tableProperties.getMergeType().equals(MergeTableProperties.MergeType.updateIntoArray) || tableProperties.getIsArray()) {
				boolean intoArray = tableProperties.getMergeType().equals(MergeTableProperties.MergeType.updateIntoArray);
				List<MergeTableProperties> children = tableProperties.getChildren();
				if (children != null) {
					for (MergeTableProperties ch : children) {
						if (!ch.getIsArray()) {
							ch.setArray(true);

//							TapLogger.warn(TAG, "Fixed merge table properties, set array to true when mergeType is updateIntoArray, table: " + ch.getTableName() + " targetPath: " + ch.getTargetPath());
						}
						if (ch.getArrayPath() == null) {
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
		initSourceNodeLevelMap(null, 1);
		this.mergeMode = ((MergeTableNode) getNode()).getMergeMode();
		this.firstLevelMergeNodeIds = new HashSet<>();
		List<MergeTableProperties> mergeProperties = ((MergeTableNode) getNode()).getMergeProperties();
		mergeProperties.stream().map(MergeTableProperties::getId).forEach(id -> firstLevelMergeNodeIds.add(id));

		TapCreateIndexEvent mergeConfigCreateIndexEvent = generateCreateIndexEventsForTarget();
		this.createIndexEvent = new TapdataEvent();
		this.createIndexEvent.setTapEvent(mergeConfigCreateIndexEvent);
		lookupThreadNum = CommonUtils.getPropertyInt(MERGE_LOOKUP_THREAD_NUM_PROP_KEY, DEFAULT_LOOKUP_THREAD_NUM);
		obsLogger.info("Merge table processor lookup thread num: " + lookupThreadNum);
		lookupQueue = new LinkedBlockingQueue<>();
		lookupThreadPool = new ThreadPoolExecutor(lookupThreadNum, lookupThreadNum, 0L, TimeUnit.MILLISECONDS, lookupQueue,
				r -> {
					Thread thread = new Thread(r);
					thread.setName("Merge-Processor-Lookup-Thread-" + thread.getId());
					return thread;
				});
		batchProcessMetrics = new BatchProcessMetrics();
		CommonUtils.ignoreAnyError(() -> PDKIntegration.registerMemoryFetcher(memoryKey(), this), TAG);
	}

	@NotNull
	private String memoryKey() {
		return String.join("_", TAG, processorBaseContext.getTaskDto().getName(), getNode().getName());
	}

	private void initSourceNodeLevelMap(List<MergeTableProperties> mergeProperties, int level) {
		Node node = getNode();

		if (!(node instanceof MergeTableNode)) {
			return;
		}
		MergeTableNode mergeTableNode = (MergeTableNode) node;
		if (null == mergeProperties) {
			mergeProperties = mergeTableNode.getMergeProperties();
		}
		if (null == this.sourceNodeLevelMap) {
			this.sourceNodeLevelMap = new ConcurrentHashMap<>();
		}
		for (MergeTableProperties mergeProperty : mergeProperties) {
			this.sourceNodeLevelMap.put(mergeProperty.getId(), level);
			if (CollectionUtils.isNotEmpty(mergeProperty.getChildren())) {
				initSourceNodeLevelMap(mergeProperty.getChildren(), level + 1);
			}
		}
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
		initSourceNodeLevelMap(null, 1);
	}

	@Override
	protected void tryProcess(List<HazelcastProcessorBaseNode.BatchEventWrapper> tapdataEvents, Consumer<List<BatchProcessResult>> consumer) {
		long startMS = System.currentTimeMillis();
		batchProcessMetrics.nextBatchIntervalMS(System.currentTimeMillis() - lastBatchProcessFinishMS);
		List<BatchProcessResult> batchProcessResults = new ArrayList<>();
		List<CompletableFuture<Void>> lookupCfs = new ArrayList<>();
		List<BatchEventWrapper> batchCache = new ArrayList<>();
		Boolean lastNeedCache = null;
		if (this.createIndexEvent != null) {
			BatchProcessResult batchProcessResult = new BatchProcessResult(new BatchEventWrapper(this.createIndexEvent), null);
			batchProcessResults.add(batchProcessResult);
			acceptIfNeed(consumer, batchProcessResults, lookupCfs);
			batchProcessResults.clear();
			this.createIndexEvent = null;
		}
		for (BatchEventWrapper batchEventWrapper : tapdataEvents) {
			TapdataEvent tapdataEvent = batchEventWrapper.getTapdataEvent();
			boolean needCache = needCache(tapdataEvent);
			if (null == lastNeedCache) {
				lastNeedCache = needCache;
			}
			boolean needLookup = needLookup(tapdataEvent);
			if (!tapdataEvent.isDML() || !Boolean.valueOf(needCache).equals(lastNeedCache)) {
				if (lastNeedCache) {
					doBatchCache(batchCache);
				}
				for (BatchEventWrapper eventWrapper : batchCache) {
					String preTableName = getPreTableName(eventWrapper.getTapdataEvent());
					batchProcessResults.add(new BatchProcessResult(eventWrapper, ProcessResult.create().tableId(preTableName)));
				}
				acceptIfNeed(consumer, batchProcessResults, lookupCfs);
				batchCache.clear();
				batchProcessResults.clear();
				lookupCfs.clear();
			}
			wrapMergeInfo(tapdataEvent);
			batchCache.add(batchEventWrapper);
			if (needLookup) {
				CompletableFuture<Void> lookupCf = lookupAndWrapMergeInfoConcurrent(tapdataEvent);
				lookupCfs.add(lookupCf);
			}
			lastNeedCache = needCache;
		}
		if (CollectionUtils.isNotEmpty(batchCache)) {
			if (null != lastNeedCache && lastNeedCache) {
				doBatchCache(batchCache);
			}
			for (BatchEventWrapper eventWrapper : batchCache) {
				String preTableName = getPreTableName(eventWrapper.getTapdataEvent());
				batchProcessResults.add(new BatchProcessResult(eventWrapper, ProcessResult.create().tableId(preTableName)));
			}
			acceptIfNeed(consumer, batchProcessResults, lookupCfs);
		}
		batchProcessMetrics.processCost(System.currentTimeMillis() - startMS, tapdataEvents.size());
		this.lastBatchProcessFinishMS = System.currentTimeMillis();
	}

	private void acceptIfNeed(Consumer<List<BatchProcessResult>> consumer, List<BatchProcessResult> batchProcessResults, List<CompletableFuture<Void>> lookupCfs) {
		batchProcessResults = batchProcessResults.stream().filter(batchProcessResult -> {
			TapdataEvent tapdataEvent = batchProcessResult.getBatchEventWrapper().getTapdataEvent();
			if (tapdataEvent.isDML()) {
				String preNodeId = getPreNodeId(batchProcessResult.getBatchEventWrapper().getTapdataEvent());
				Integer level = sourceNodeLevelMap.get(preNodeId);
				return !isSubTableFirstMode() || level == null || level <= 1;
			}
			return true;
		}).collect(Collectors.toList());
		if (CollectionUtils.isNotEmpty(batchProcessResults)) {
			if (CollectionUtils.isNotEmpty(lookupCfs)) {
				try {
					CompletableFuture.allOf(lookupCfs.toArray(new CompletableFuture[0])).join();
				} catch (Exception e) {
					errorHandle(e);
					return;
				}
			}
			consumer.accept(batchProcessResults);
		}
	}

	private CompletableFuture<Void> lookupAndWrapMergeInfoConcurrent(TapdataEvent tapdataEvent) {
		Runnable runnable = () -> {
			long startMS = System.currentTimeMillis();
			MergeInfo mergeInfo = wrapMergeInfo(tapdataEvent);
			List<MergeLookupResult> mergeLookupResults = lookup(tapdataEvent);
			mergeInfo.setMergeLookupResults(mergeLookupResults);
			batchProcessMetrics.lookupCost(System.currentTimeMillis() - startMS);
		};
		return CompletableFuture.runAsync(runnable, lookupThreadPool);
	}

	private void doBatchCache(List<BatchEventWrapper> batchCache) {
		long startMS = System.currentTimeMillis();
		if (CollectionUtils.isNotEmpty(batchCache)) {
			cache(batchCache.stream().map(BatchEventWrapper::getTapdataEvent).collect(Collectors.toList()));
		}
		batchProcessMetrics.cacheCost(System.currentTimeMillis() - startMS, batchCache.size());
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		if (this.createIndexEvent != null) {
			consumer.accept(this.createIndexEvent, null);
			this.createIndexEvent = null;
		}
		if (!tapdataEvent.isDML()) {
			consumer.accept(tapdataEvent, null);
			return;
		}
		String preTableName = getPreTableName(tapdataEvent);
		if (needCache(tapdataEvent)) {
			cache(tapdataEvent);
		}
		MergeInfo mergeInfo = wrapMergeInfo(tapdataEvent);
		if (needLookup(tapdataEvent)) {
			List<MergeLookupResult> mergeLookupResults = lookup(tapdataEvent);
			mergeInfo.setMergeLookupResults(mergeLookupResults);
		}
		consumer.accept(tapdataEvent, ProcessResult.create().tableId(preTableName));
	}

	private MergeInfo wrapMergeInfo(TapdataEvent tapdataEvent) {
		String preNodeId = getPreNodeId(tapdataEvent);
		if (!preNodeIdPdkMergeTablePropertieMap.containsKey(preNodeId)) {
			MergeTableProperties currentMergeTableProperty = this.mergeTablePropertiesMap.get(preNodeId);
			io.tapdata.pdk.apis.entity.merge.MergeTableProperties pdkMergeTableProperties = copyMergeTableProperty(currentMergeTableProperty);
			preNodeIdPdkMergeTablePropertieMap.put(preNodeId, pdkMergeTableProperties);
		}
		if (!(tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY) instanceof MergeInfo)) {
			MergeInfo mergeInfo = new MergeInfo();
			mergeInfo.setCurrentProperty(preNodeIdPdkMergeTablePropertieMap.get(preNodeId));
			mergeInfo.setLevel(this.sourceNodeLevelMap.get(preNodeId));
			tapdataEvent.getTapEvent().addInfo(MergeInfo.EVENT_INFO_KEY, mergeInfo);
			return mergeInfo;
		} else {
			return (MergeInfo) tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
		}
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
				int mergeCacheInMemSize = CommonUtils.getPropertyInt(MERGE_CACHE_IN_MEM_SIZE_PROP_KEY, DEFAULT_MERGE_CACHE_IN_MEM_SIZE);
				externalStorageDto.setInMemSize(mergeCacheInMemSize);
				externalStorageDto.setWriteDelaySeconds(1);
				externalStorageDto.setTtlDay(0);
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
			throw new RuntimeException("From node id list is empty, " + tapdataEvent);
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
		if (isInvalidOperation(tapdataEvent)) return false;
		String op = getOp(tapdataEvent);
		if (op.equals(OperationType.DELETE.getOp())) {
			return false;
		}
		String preNodeId = getPreNodeId(tapdataEvent);
		boolean existsInLookupMap = this.lookupMap.containsKey(preNodeId);
		if (existsInLookupMap && SyncStage.INITIAL_SYNC.equals(syncStage)
				&& (isMainTableFirstMode() || (isSubTableFirstMode() && !isFirstMergeLevel(preNodeId)))) {
			return false;
		}
		return existsInLookupMap;
	}

	private boolean isFirstMergeLevel(String preNodeId) {
		return firstLevelMergeNodeIds.contains(preNodeId);
	}

	private boolean isSubTableFirstMode() {
		return MergeTableNode.SUB_TABLE_FIRST_MERGE_MODE.equals(mergeMode);
	}

	private boolean isMainTableFirstMode() {
		return MergeTableNode.MAIN_TABLE_FIRST_MERGE_MODE.equals(mergeMode);
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

	private void cache(List<TapdataEvent> tapdataEvents) {
		if (null == tapdataEvents) {
			return;
		}
//		checkBatchCache(tapdataEvents);
		Map<String, List<TapdataEvent>> preNodeIdPartitionEventMap = new HashMap<>();
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			String preNodeId = getPreNodeId(tapdataEvent);
			List<TapdataEvent> partitionEvents;
			if (!preNodeIdPartitionEventMap.containsKey(preNodeId)) {
				partitionEvents = new ArrayList<>();
				preNodeIdPartitionEventMap.put(preNodeId, partitionEvents);
			} else {
				partitionEvents = preNodeIdPartitionEventMap.get(preNodeId);
			}
			partitionEvents.add(tapdataEvent);
		}
		for (Map.Entry<String, List<TapdataEvent>> entry : preNodeIdPartitionEventMap.entrySet()) {
			List<TapdataEvent> samePreNodeIdEvents = entry.getValue();
			ConstructIMap<Document> hazelcastConstruct = getHazelcastConstruct(getPreNodeId(samePreNodeIdEvents.get(0)));
			MergeTableProperties mergeProperty = getMergeProperty(samePreNodeIdEvents.get(0));
			try {
				String lastOp = "";
				List<TapdataEvent> dispatchByOpEvents = new ArrayList<>();
				for (TapdataEvent samePreNodeIdEvent : samePreNodeIdEvents) {
					String op = getOp(samePreNodeIdEvent);
					if (StringUtils.isNotBlank(lastOp) && !lastOp.equals(op)) {
						handleCacheByOp(lastOp, dispatchByOpEvents, mergeProperty, hazelcastConstruct);
						dispatchByOpEvents.clear();
					}
					dispatchByOpEvents.add(samePreNodeIdEvent);
					lastOp = op;
				}
				if (CollectionUtils.isNotEmpty(dispatchByOpEvents)) {
					TapdataEvent firstEvent = dispatchByOpEvents.get(0);
					String op = getOp(firstEvent);
					handleCacheByOp(op, dispatchByOpEvents, mergeProperty, hazelcastConstruct);
					dispatchByOpEvents.clear();
				}
			} catch (Exception e) {
				if (e instanceof TapCodeException) {
					throw (TapCodeException) e;
				} else {
					throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHE_UNKNOWN_ERROR, e);
				}
			}
		}
	}

	private void handleCacheByOp(String op, List<TapdataEvent> events, MergeTableProperties mergeProperty, ConstructIMap<Document> hazelcastConstruct) {
		OperationType operationType = OperationType.fromOp(op);
		switch (operationType) {
			case INSERT:
			case UPDATE:
				try {
					upsertCache(events, mergeProperty, hazelcastConstruct);
				} catch (Exception e) {
					throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHE_UNKNOWN_ERROR, "First event: " + events.get(0), e);
				}
				break;
			case DELETE:
				events.forEach(event -> {
					try {
						deleteCache(event, mergeProperty, hazelcastConstruct);
					} catch (Exception e) {
						throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHE_UNKNOWN_ERROR, "Event: " + event, e);
					}
				});
				break;
		}
	}

	private void checkBatchCache(List<TapdataEvent> tapdataEvents) {
		TapdataEvent tapdataEvent = tapdataEvents.stream().filter(event -> {
			String op = getOp(event);
			OperationType operationType = OperationType.fromOp(op);
			return OperationType.INSERT != operationType;
		}).findFirst().orElse(null);
		if (null != tapdataEvent) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.INVALID_OPERATION, String.format("Invalid operation: %s", getOp(tapdataEvent)));
		}
	}

	private void upsertCache(TapdataEvent tapdataEvent, MergeTableProperties mergeTableProperty, ConstructIMap<Document> hazelcastConstruct) {
		Map<String, Object> after = getAfter(tapdataEvent);
		String joinValueKey = getJoinValueKeyBySource(after, mergeTableProperty, hazelcastConstruct);
		String encodeJoinValueKey = encode(joinValueKey);
		String pkOrUniqueValueKey = getPkOrUniqueValueKey(after, mergeTableProperty, hazelcastConstruct);
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

	private void upsertCache(List<TapdataEvent> tapdataEvents, MergeTableProperties mergeTableProperties, ConstructIMap<Document> hazelcastConstruct) {
		Map<String, TapdataEvent> joinValueKeyTapdataEventMap = new HashMap<>();
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			Map<String, Object> after = getAfter(tapdataEvent);
			String joinValueKeyBySource = getJoinValueKeyBySource(after, mergeTableProperties, hazelcastConstruct);
			String encodeJoinValueKey = encode(joinValueKeyBySource);
			joinValueKeyTapdataEventMap.put(encodeJoinValueKey, tapdataEvent);
		}
		Map<String, Object> groupByJoinKeyValues;
		try {
			groupByJoinKeyValues = hazelcastConstruct.findAll(joinValueKeyTapdataEventMap.keySet());
		} catch (Exception e) {
			if (null != e.getCause() && e.getCause() instanceof InterruptedException) {
				return;
			}
			throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHE_FIND_BY_JOIN_KEYS_FAILED, e);
		}
		Map<String, Document> insertMap = new HashMap<>();
		for (String joinValueKey : joinValueKeyTapdataEventMap.keySet()) {
			Object groupByJoinKeyValue = groupByJoinKeyValues.get(joinValueKey);
			if (null == groupByJoinKeyValue) {
				groupByJoinKeyValue = new Document();
			}
			if (groupByJoinKeyValue instanceof Document) {
				TapdataEvent tapdataEvent = joinValueKeyTapdataEventMap.get(joinValueKey);
				Map<String, Object> after = getAfter(tapdataEvent);
				String pkOrUniqueValueKey = getPkOrUniqueValueKey(after, mergeTableProperties, hazelcastConstruct);
				String encodePkOrUniqueValueKey = encode(pkOrUniqueValueKey);
				((Document) groupByJoinKeyValue).put(encodePkOrUniqueValueKey, after);
				insertMap.put(joinValueKey, (Document) groupByJoinKeyValue);
			}
		}
		try {
			hazelcastConstruct.insertMany(insertMap);
		} catch (Exception e) {
			if (null != e.getCause() && e.getCause() instanceof InterruptedException) {
				return;
			}
			throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHES_FAILED, e);
		}
	}

	private void deleteCache(TapdataEvent tapdataEvent, MergeTableProperties mergeTableProperty, ConstructIMap<Document> hazelcastConstruct) throws Exception {
		Map<String, Object> before = getBefore(tapdataEvent);
		String joinValueKey = getJoinValueKeyBySource(before, mergeTableProperty, hazelcastConstruct);
		String encodeJoinValueKey = encode(joinValueKey);
		String pkOrUniqueValueKey = getPkOrUniqueValueKey(before, mergeTableProperty, hazelcastConstruct);
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
		} else {
			hazelcastConstruct.upsert(encodeJoinValueKey, groupByJoinKeyValues);
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

	private String getJoinValueKeyBySource(Map<String, Object> data, MergeTableProperties mergeProperty, ConstructIMap<Document> hazelcastConstruct) {
		List<Map<String, String>> joinKeys = mergeProperty.getJoinKeys();
		List<String> joinKeyList = getJoinKeys(joinKeys, JoinConditionType.SOURCE);
		if (CollectionUtils.isEmpty(joinKeyList)) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.MISSING_SOURCE_JOIN_KEY_CONFIG, String.format("Map name: %s, Merge property: %s", hazelcastConstruct.getName(), mergeProperty));
		}
		if (MapUtils.isEmpty(data)) {
			return "";
		}
		List<String> values = new ArrayList<>();
		for (String joinKey : joinKeyList) {
			Object value = MapUtilV2.getValueByKey(data, joinKey);
			if (value instanceof NotExistsNode) {
				throw new TapCodeException(TaskMergeProcessorExCode_16.JOIN_KEY_VALUE_NOT_EXISTS, String.format("- Map name: %s\n- Join key: %s\n- Data: %s", hazelcastConstruct.getName(), joinKey, data));
			}
			values.add(String.valueOf(value));
		}
		return String.join("_", values);
	}

	private String getJoinValueKeyByTarget(Map<String, Object> data, MergeTableProperties mergeProperty, MergeTableProperties lastMergeProperty, ConstructIMap<Document> hazelcastConstruct) {
		List<Map<String, String>> joinKeys = mergeProperty.getJoinKeys();
		List<String> joinKeyList = getJoinKeys(joinKeys, JoinConditionType.TARGET);
		if (CollectionUtils.isEmpty(joinKeyList)) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.MISSING_TARGET_JOIN_KEY_CONFIG, String.format("Map name: %s, Merge property: %s", hazelcastConstruct, mergeProperty));
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

	private String getPkOrUniqueValueKey(Map<String, Object> data, MergeTableProperties mergeProperty, ConstructIMap<Document> hazelcastConstruct) {
		String sourceNodeId = mergeProperty.getId();
		List<String> pkOrUniqueFields = this.sourcePkOrUniqueFieldMap.get(sourceNodeId);
		if (MapUtils.isEmpty(data)) {
			return "";
		}
		List<String> values = new ArrayList<>();
		for (String pkOrUniqueField : pkOrUniqueFields) {
			Object value = MapUtilV2.getValueByKey(data, pkOrUniqueField);
			if (value instanceof NotExistsNode) {
				throw new TapCodeException(TaskMergeProcessorExCode_16.PK_OR_UNIQUE_VALUE_NOT_EXISTS, String.format("- Map name: %s\n- Pk or unique field: %s\n- Data: %s", hazelcastConstruct.getName(), pkOrUniqueFields, data));
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

	private String getPreTableName(TapdataEvent tapdataEvent) {
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
		return preTableName;
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
			String joinValueKey = getJoinValueKeyByTarget(data, childMergeProperty, mergeTableProperties, hazelcastConstruct);
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

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create();
		dataMap.kv("lookup thread pool size", lookupThreadNum);
		dataMap.kv("lookup runnable queue", null == lookupQueue ? 0 : lookupQueue.size());
		dataMap.kv("last batch metrics", batchProcessMetrics.toString());
		return dataMap;
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
		/*if (!(node instanceof MergeTableNode)) return;
		ExternalStorageDto externalStorage = ExternalStorageUtil.getExternalStorage(node);
		recursiveClearCache(externalStorage, ((MergeTableNode) node).getMergeProperties(), HazelcastUtil.getInstance());*/
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
		try {
			if (MapUtils.isNotEmpty(mergeCacheMap)) {
				for (ConstructIMap<Document> constructIMap : mergeCacheMap.values()) {
					try {
						obsLogger.info("Destroy merge cache resource: {}", constructIMap.getName());
						constructIMap.destroy();
					} catch (Exception e) {
						obsLogger.warn("Destroy merge cache failed, name: {}, error message: {}\nStack: {}", constructIMap.getName(), e.getMessage(), Log4jUtil.getStackString(e));
					}
				}
			}
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(lookupThreadPool).ifPresent(ExecutorService::shutdownNow), TAG);
			CommonUtils.ignoreAnyError(() -> PDKIntegration.unregisterMemoryFetcher(memoryKey()), TAG);
		} finally {
			super.doClose();
		}
	}

	private static class BatchProcessMetrics {
		private long cacheCostMS;
		private long cacheRow;
		private final Map<String, LookupMetrics> lookupCostMSMap;
		private long processCostMS;
		private long processRow;
		private long nextBatchIntervalMS;

		public BatchProcessMetrics() {
			this.cacheCostMS = 0L;
			this.cacheRow = 0L;
			this.lookupCostMSMap = new ConcurrentHashMap<>();
		}

		public void cacheCost(long cacheCostMS, long cacheRow) {
			this.cacheCostMS = cacheCostMS;
			this.cacheRow = cacheRow;
		}

		public void lookupCost(long lookupCostMS) {
			String threadName = Thread.currentThread().getName();
			this.lookupCostMSMap.putIfAbsent(threadName, new LookupMetrics(1L, lookupCostMS));
			this.lookupCostMSMap.computeIfPresent(threadName, (k, v) -> {
				v.cost(lookupCostMS);
				return v;
			});
		}

		public void processCost(long processCostMS, long processRow) {
			this.processCostMS = processCostMS;
			this.processRow = processRow;
		}

		public void nextBatchIntervalMS(long nextBatchIntervalMS) {
			this.nextBatchIntervalMS = nextBatchIntervalMS;
		}

		private double ms2Sec(long ms) {
			return BigDecimal.valueOf(ms).divide(BigDecimal.valueOf(1000), 2, RoundingMode.HALF_UP).doubleValue();
		}

		private double qps(long row, long ms) {
			double sec = ms2Sec(ms);
			return sec == 0L ? 0L : BigDecimal.valueOf(row).divide(BigDecimal.valueOf(sec), 2, RoundingMode.HALF_UP).doubleValue();
		}

		@Override
		public String toString() {
			double lastCacheQps = qps(cacheRow, cacheCostMS);
			double lastLookupQps = this.lookupCostMSMap.values().stream().mapToDouble(l -> qps(l.getRow(), l.getCostMS())).sum();
			double processQps = qps(processRow, processCostMS);
			return String.format("cache qps: %s, lookup qps: %s, process qps: %s, process row: %s, next batch interval ms: %s", lastCacheQps, lastLookupQps, processQps, processRow, nextBatchIntervalMS);
		}
	}

	@Getter
	private static class LookupMetrics {
		private long row;
		private long costMS;

		public LookupMetrics(long row, long costMS) {
			this.row = row;
			this.costMS = costMS;
		}

		public void cost(long costMS) {
			this.costMS += costMS;
			this.row++;
		}
	}
}

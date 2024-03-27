package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.entity.codec.filter.MapIteratorEx;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TaskMergeProcessorExCode_16;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import io.tapdata.utils.AppType;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonType;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.StopWatch;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
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
	public static final String UPDATE_JOIN_KEY_VALUE_CACHE_TABLE_SUFFIX = "UJKV";
	public static final int DEFAULT_UPDATE_JOIN_KEY_VALUE_CACHE_IN_MEM_SIZE = 10;
	public static final String UPDATE_JOIN_KEY_VALUE_CACHE_IN_MEM_SIZE_PROP_KEY = "UPDATE_JOIN_KEY_VALUE_CACHE_IN_MEM_SIZE";
	public static final String HANDLE_UPDATE_JOIN_KEY_THREAD_NUM_PROP_KEY = "HANDLE_UPDATE_JOIN_KEY_THREAD_NUM";
	public static final int DEFAULT_UPDATE_JOIN_KEY_THREAD_NUM = 4;
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
	private ExecutorService handleUpdateJoinKeyThreadPool;
	private int lookupThreadNum;
	private BlockingQueue<Runnable> lookupQueue;
	private Set<String> firstLevelMergeNodeIds;
	private BatchProcessMetrics batchProcessMetrics;
	private long lastBatchProcessFinishMS;
	MapIteratorEx mapIterator;
	private Map<String, Set<String>> shareJoinKeysMap;
	private Map<String, MergeTablePropertyReference> mergeTablePropertyReferenceMap;
	private Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap;
	private Map<String, EnableUpdateJoinKey> enableUpdateJoinKeyMap;
	private ObsLogger nodeLogger;

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
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		if (AppType.currentType().isCloud())
			externalStorageDto = ExternalStorageUtil.getTargetNodeExternalStorage(
					processorBaseContext.getNode(),
					processorBaseContext.getEdges(),
					clientMongoOperator,
					processorBaseContext.getNodes());
		selfCheckNode(getNode());
		initRuntimeParameters();

		TapCreateIndexEvent mergeConfigCreateIndexEvent = generateCreateIndexEventsForTarget();
		this.createIndexEvent = new TapdataEvent();
		this.createIndexEvent.setTapEvent(mergeConfigCreateIndexEvent);
		initLookUpThreadPool();
		initHandleUpdateJoinKeyThreadPool();
		this.mapIterator = new AllLayerMapIterator();
		batchProcessMetrics = new BatchProcessMetrics();
		CommonUtils.ignoreAnyError(() -> PDKIntegration.registerMemoryFetcher(memoryKey(), this), TAG);
		nodeLogger = ObsLoggerFactory.getInstance().getObsLogger(processorBaseContext.getTaskDto().getId().toHexString(), getNode().getId());
	}

	protected void initFirstLevelIds() {
		this.mergeMode = ((MergeTableNode) getNode()).getMergeMode();
		this.firstLevelMergeNodeIds = new HashSet<>();
		List<MergeTableProperties> mergeProperties = ((MergeTableNode) getNode()).getMergeProperties();
		mergeProperties.stream().map(MergeTableProperties::getId).forEach(id -> firstLevelMergeNodeIds.add(id));
	}

	protected void initLookUpThreadPool() {
		lookupThreadNum = CommonUtils.getPropertyInt(MERGE_LOOKUP_THREAD_NUM_PROP_KEY, DEFAULT_LOOKUP_THREAD_NUM);
		obsLogger.info("Merge table processor lookup thread num: " + lookupThreadNum);
		lookupQueue = new LinkedBlockingQueue<>();
		lookupThreadPool = new ThreadPoolExecutor(lookupThreadNum, lookupThreadNum, 0L, TimeUnit.MILLISECONDS, lookupQueue,
				r -> {
					Thread thread = new Thread(r);
					thread.setName("Merge-Processor-Lookup-Thread-" + thread.getId());
					return thread;
				});
	}

	protected void initHandleUpdateJoinKeyThreadPool() {
		int handleUpdateJoinKeyThreadNum = CommonUtils.getPropertyInt(HANDLE_UPDATE_JOIN_KEY_THREAD_NUM_PROP_KEY, DEFAULT_UPDATE_JOIN_KEY_THREAD_NUM);
		obsLogger.info("Merge table processor handle update join key thread num: " + handleUpdateJoinKeyThreadNum);
		LinkedBlockingQueue<Runnable> handleUpdateJoinKeyQueue = new LinkedBlockingQueue<>();
		handleUpdateJoinKeyThreadPool = new ThreadPoolExecutor(handleUpdateJoinKeyThreadNum, handleUpdateJoinKeyThreadNum, 0L, TimeUnit.MILLISECONDS, handleUpdateJoinKeyQueue,
				r -> {
					Thread thread = new Thread(r);
					thread.setName("Merge-Processor-Handle-Update-Join-Key-Thread-" + thread.getId());
					return thread;
				});
	}

	@NotNull
	private String memoryKey() {
		return String.join("_", TAG, processorBaseContext.getTaskDto().getName(), getNode().getName());
	}

	protected void initSourceNodeLevelMap(List<MergeTableProperties> mergeProperties, int level) {
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
		initRuntimeParameters();
	}

	protected void initRuntimeParameters() {
		initFirstLevelIds();
		initMergeTableProperties();
		initLookupMergeProperties();
		initMergeCache();
		initSourceNodeMap(null);
		initSourceConnectionMap(null);
		initSourcePkOrUniqueFieldMap(null);
		initSourceNodeLevelMap(null, 1);
		initShareJoinKeys();
		initMergeTablePropertyReferenceMap();
		initCheckJoinKeyUpdateCacheMap();
	}

	@Override
	protected void tryProcess(List<HazelcastProcessorBaseNode.BatchEventWrapper> tapdataEvents, Consumer<List<BatchProcessResult>> consumer) {
		loggerBeforeProcess(tapdataEvents);
		StopWatch stopWatch = new StopWatch();
		List<CompletableFuture<Void>> lookupCfs = new ArrayList<>();
		List<BatchEventWrapper> batchCache = new ArrayList<>();

		try {
			stopWatch.start();
			batchProcessMetrics.nextBatchIntervalMS(System.currentTimeMillis() - lastBatchProcessFinishMS);
			List<BatchProcessResult> batchProcessResults = new ArrayList<>();
			if (this.createIndexEvent != null) {
				BatchProcessResult batchProcessResult = new BatchProcessResult(new BatchEventWrapper(this.createIndexEvent), null);
				batchProcessResults.add(batchProcessResult);
				acceptIfNeed(consumer, batchProcessResults, lookupCfs);
				batchProcessResults.clear();
				this.createIndexEvent = null;
			}
			handleBatchUpdateJoinKey(tapdataEvents);
			for (BatchEventWrapper batchEventWrapper : tapdataEvents) {
				if (Boolean.TRUE.equals(needCache(batchEventWrapper.getTapdataEvent()))) {
					batchCache.add(batchEventWrapper);
				}
				wrapMergeInfo(batchEventWrapper.getTapdataEvent());
			}
			if (CollectionUtils.isNotEmpty(batchCache)) {
				doBatchCache(batchCache);
				loggerBatchUpdateCache(batchCache);
			}
			doBatchLookUpConcurrent(tapdataEvents, lookupCfs);
			for (BatchEventWrapper batchEventWrapper : tapdataEvents) {
				String preTableName = getPreTableName(batchEventWrapper.getTapdataEvent());
				batchProcessResults.add(new BatchProcessResult(batchEventWrapper, ProcessResult.create().tableId(preTableName)));
			}
			acceptIfNeed(consumer, batchProcessResults, lookupCfs);
		} finally {
			stopWatch.stop();
			batchProcessMetrics.processCost(stopWatch.getTotalTimeMillis(), tapdataEvents.size());
			this.lastBatchProcessFinishMS = System.currentTimeMillis();

			// Let jvm gc
			lookupCfs = null;
			batchCache = null;
		}
	}

	protected void loggerBeforeProcess(List<BatchEventWrapper> tapdataEvents) {
		if (null == nodeLogger) return;
		if (nodeLogger.isDebugEnabled()) {
			nodeLogger.debug("[{}] Process merge event, size: {}", System.currentTimeMillis(), tapdataEvents.size());
			for (BatchEventWrapper tapdataEvent : tapdataEvents) {
				nodeLogger.debug("[{}] Tapdata event: {}", System.currentTimeMillis(), tapdataEvent.getTapdataEvent().getTapEvent());
			}
		}
	}

	protected void doBatchLookUpConcurrent(List<BatchEventWrapper> batchCache, List<CompletableFuture<Void>> lookupCfs) {
		if (null == batchCache) return;
		if (null == lookupCfs) throw new TapCodeException(TaskMergeProcessorExCode_16.LOOKUP_COMPLETABLE_FUTURE_LIST_IS_NULL);
		batchCache.forEach(eventWrapper -> {
			if (Boolean.TRUE.equals(needLookup(eventWrapper.getTapdataEvent()))) {
				CompletableFuture<Void> lookupCf = lookupAndWrapMergeInfoConcurrent(eventWrapper.getTapdataEvent());
				lookupCfs.add(lookupCf);
			}
		});
	}

	protected void loggerBatchUpdateCache(List<BatchEventWrapper> batchCache) {
		if(null == nodeLogger) return;
		if (nodeLogger.isDebugEnabled()) {
			nodeLogger.debug("[{}] Do batch update cache, size: {}", System.currentTimeMillis(), batchCache.size());
			for (BatchEventWrapper eventWrapper : batchCache) {
				nodeLogger.debug("[{}] Cache event: {}", System.currentTimeMillis(), eventWrapper.getTapdataEvent());
			}
		}
	}

	protected void acceptIfNeed(Consumer<List<BatchProcessResult>> consumer, List<BatchProcessResult> batchProcessResults, List<CompletableFuture<Void>> lookupCfs) {
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

	protected CompletableFuture<Void> lookupAndWrapMergeInfoConcurrent(TapdataEvent tapdataEvent) {
		Runnable runnable = () -> {
			StopWatch stopWatch = new StopWatch();
			List<MergeLookupResult> mergeLookupResults = null;
			try {
				stopWatch.start();
				MergeInfo mergeInfo = wrapMergeInfo(tapdataEvent);
				mergeLookupResults = lookup(tapdataEvent);
				mergeInfo.setMergeLookupResults(mergeLookupResults);
			} finally {
				stopWatch.stop();
				if (null != nodeLogger && nodeLogger.isDebugEnabled()) {
					nodeLogger.debug("[{}] Do lookup, cost: {} ms, event: {}, lookup result: {}",
							System.currentTimeMillis(), stopWatch.getTotalTimeMillis(), tapdataEvent,
							null == mergeLookupResults ? 0 : mergeLookupResults.size());
				}
				batchProcessMetrics.lookupCost(stopWatch.getTotalTimeMillis());
			}
		};
		return CompletableFuture.runAsync(runnable, lookupThreadPool);
	}

	protected void doBatchCache(List<BatchEventWrapper> batchCache) {
		StopWatch stopWatch = new StopWatch();
		try {
			stopWatch.start();
			if (CollectionUtils.isNotEmpty(batchCache)) {
				cache(batchCache.stream().map(BatchEventWrapper::getTapdataEvent).collect(Collectors.toList()));
			}
		} finally {
			stopWatch.stop();
			batchProcessMetrics.cacheCost(stopWatch.getTotalTimeMillis(), batchCache.size());
		}
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
		handleUpdateJoinKey(tapdataEvent);
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

	protected MergeInfo wrapMergeInfo(TapdataEvent tapdataEvent) {
		String preNodeId = getPreNodeId(tapdataEvent);
		preNodeIdPdkMergeTablePropertieMap.computeIfAbsent(preNodeId, k -> {
			MergeTableProperties currentMergeTableProperty = this.mergeTablePropertiesMap.get(k);
			return copyMergeTableProperty(currentMergeTableProperty);
		});
		if (!(tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY) instanceof MergeInfo)) {
			MergeInfo mergeInfo = new MergeInfo();
			mergeInfo.setCurrentProperty(preNodeIdPdkMergeTablePropertieMap.get(preNodeId));
			mergeInfo.setLevel(this.sourceNodeLevelMap.get(preNodeId));
			mergeInfo.setSharedJoinKeys(this.shareJoinKeysMap.get(preNodeId));
			tapdataEvent.getTapEvent().addInfo(MergeInfo.EVENT_INFO_KEY, mergeInfo);
			return mergeInfo;
		} else {
			return (MergeInfo) tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
		}
	}

	protected Node<?> getPreNode(String preNodeId) {
		return preNodeMap.computeIfAbsent(preNodeId, k -> {
			Node<?> foundNode = processorBaseContext.getNodes().stream().filter(n -> n.getId().equals(preNodeId)).findFirst().orElse(null);
			if (null == foundNode)
				throw new TapCodeException(TaskMergeProcessorExCode_16.CANNOT_GET_PRENODE_BY_ID, String.format("Node id: %s", preNodeId));
			return foundNode;
		});
	}

	protected void initMergeTableProperties() {
		this.mergeTablePropertiesMap = new HashMap<>();
		Node<?> node = processorBaseContext.getNode();
		List<MergeTableProperties> mergeTableProperties;
		if (node instanceof MergeTableNode) {
			mergeTableProperties = ((MergeTableNode) node).getMergeProperties();
		} else {
			throw new TapCodeException(TaskMergeProcessorExCode_16.WRONG_NODE_TYPE, "Expect MergeTableNode, but got: " + node.getClass().getSimpleName());
		}
		recursiveInitMergeTableProperties(mergeTableProperties, null);
	}

	private void recursiveInitMergeTableProperties(List<MergeTableProperties> mergeTableProperties, MergeTableProperties parentProperties) {
		if (CollectionUtils.isEmpty(mergeTableProperties)) return;
		for (MergeTableProperties mergeTableProperty : mergeTableProperties) {
			String id = mergeTableProperty.getId();
			if (null != parentProperties) {
				mergeTableProperty.setParentId(parentProperties.getId());
			}
			this.mergeTablePropertiesMap.put(id, mergeTableProperty);
			List<MergeTableProperties> children = mergeTableProperty.getChildren();
			if (CollectionUtils.isNotEmpty(children)) {
				recursiveInitMergeTableProperties(children, mergeTableProperty);
			}
		}
	}

	protected void initMergeCache() {
		if (isInitialSyncTask() && !isSubTableFirstMode()) return;
		this.mergeCacheMap = new HashMap<>();
		if (MapUtils.isEmpty(this.lookupMap)) {
			return;
		}
		for (List<MergeTableProperties> lookupList : this.lookupMap.values()) {
			for (MergeTableProperties mergeProperty : lookupList) {
				String cacheName;
				try {
					cacheName = getMergeCacheName(mergeProperty.getId(), mergeProperty.getTableName());
				} catch (Exception e) {
					throw new TapCodeException(TaskMergeProcessorExCode_16.INIT_MERGE_CACHE_GET_CACHE_NAME_FAILED, e);
				}
				if (StringUtils.isBlank(cacheName)) {
					break;
				}
				int mergeCacheInMemSize = CommonUtils.getPropertyInt(MERGE_CACHE_IN_MEM_SIZE_PROP_KEY, DEFAULT_MERGE_CACHE_IN_MEM_SIZE);
				ExternalStorageDto externalStorageDtoCopy = copyExternalStorage(mergeCacheInMemSize);
				ConstructIMap<Document> hazelcastConstruct = buildConstructIMap(jetContext.hazelcastInstance(), TAG, cacheName, externalStorageDtoCopy);
				this.mergeCacheMap.put(mergeProperty.getId(), hazelcastConstruct);
				obsLogger.info("Create merge cache imap name: {}, external storage: {}", cacheName, externalStorageDtoCopy);
			}
		}
	}

	protected void initMergeTablePropertyReferenceMap() {
		if (null == mergeTablePropertiesMap) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.INIT_MERGE_PROPERTY_RREFERENCE_FAILED_MERGE_PROPERTIES_MAP_IS_NULL);
		}
		this.mergeTablePropertyReferenceMap = new HashMap<>();
		this.enableUpdateJoinKeyMap = new HashMap<>();
		for (Map.Entry<String, MergeTableProperties> entry : this.mergeTablePropertiesMap.entrySet()) {
			String id = entry.getKey();
			MergeTableProperties mergeTableProperties = entry.getValue();
			MergeTablePropertyReference mergeTablePropertyReference = new MergeTablePropertyReference();
			mergeTablePropertyReferenceMap.put(id, mergeTablePropertyReference);
			this.enableUpdateJoinKeyMap.putIfAbsent(id, new EnableUpdateJoinKey());
			if (Boolean.TRUE.equals(mergeTableProperties.getEnableUpdateJoinKeyValue()) && !firstLevelMergeNodeIds.contains(id)) {
				this.enableUpdateJoinKeyMap.computeIfPresent(id, (k, v) -> {
					v.enableParent();
					return v;
				});
			}
			List<JoinKeyReference> joinKeyParentReferences = analyzeParentReference(id);
			if (CollectionUtils.isNotEmpty(joinKeyParentReferences)) {
				mergeTablePropertyReference.setParentJoinKeyReferences(joinKeyParentReferences);
			}
			List<JoinKeyReference> joinKeyChildReferences = analyzeChildrenReference(id);
			if (CollectionUtils.isNotEmpty(joinKeyChildReferences)) {
				mergeTablePropertyReference.setChildJoinKeyReferences(joinKeyChildReferences);
			}
		}
	}

	protected void initCheckJoinKeyUpdateCacheMap() {
		this.checkJoinKeyUpdateCacheMap = new HashMap<>();
		for (Map.Entry<String, EnableUpdateJoinKey> entry : this.enableUpdateJoinKeyMap.entrySet()) {
			String id = entry.getKey();
			EnableUpdateJoinKey enableUpdateJoinKey = entry.getValue();
			if ((Boolean.TRUE.equals(enableUpdateJoinKey.isEnableParent()) || Boolean.TRUE.equals(enableUpdateJoinKey.isEnableChildren())) && !isSourceHaveBefore(id)) {
				putInCheckJoinKeyUpdateCacheMapAndWriteSign(id, getCheckUpdateJoinKeyValueCacheName(id));
			}
		}
	}

	private void putInCheckJoinKeyUpdateCacheMapAndWriteSign(String id, String cacheName) {
		List<String> joinKeyIncludePK = checkJoinKeyIncludePK(id);
		if (CollectionUtils.isNotEmpty(joinKeyIncludePK) && !isSourceHaveBefore(id)) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.BUILD_CHECK_UPDATE_JOIN_KEY_CACHE_FAILED_JOIN_KEY_INCLUDE_PK, String.format("Join key include pk, id: %s, both join key and pk: %s", id, joinKeyIncludePK));
		}
		int inMemSize = CommonUtils.getPropertyInt(UPDATE_JOIN_KEY_VALUE_CACHE_IN_MEM_SIZE_PROP_KEY, DEFAULT_UPDATE_JOIN_KEY_VALUE_CACHE_IN_MEM_SIZE);
		ExternalStorageDto externalStorageDtoCopy = copyExternalStorage(inMemSize);
		ConstructIMap<Document> constructIMap = buildConstructIMap(jetContext.hazelcastInstance(), String.join("_", TAG, UPDATE_JOIN_KEY_VALUE_CACHE_TABLE_SUFFIX), String.valueOf(cacheName.hashCode()), externalStorageDtoCopy);
		obsLogger.info("Create check join key value modify cache imap name: {}, external storage: {}", String.valueOf(cacheName.hashCode()), externalStorageDtoCopy);
		this.checkJoinKeyUpdateCacheMap.put(id, constructIMap);
		if (constructIMap.isEmpty()) {
			Document sign = new Document("original_name", cacheName);
			Node<?> preNode = getPreNode(id);
			if (null != preNode) {
				sign.append("pre_node_id", preNode.getId());
				sign.append("pre_node_name", preNode.getName());
			}
			try {
				constructIMap.insert("sign", sign);
			} catch (Exception e) {
				throw new TapCodeException(TaskMergeProcessorExCode_16.INIT_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_WRITE_SIGN_FAILED, String.format("Cache name: %s, sign document: %s", cacheName, sign), e);
			}
		}
	}

	protected List<String> checkJoinKeyIncludePK(String id) {
		MergeTableProperties mergeTableProperties = this.mergeTablePropertiesMap.get(id);
		List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
		List<String> sourceJoinKeys = getJoinKeys(joinKeys, JoinConditionType.SOURCE);
		String tableName = getTableName(getPreNode(id));
		TapTable tapTable = processorBaseContext.getTapTableMap().get(tableName);
		List<String> primaryKeys = new ArrayList<>(tapTable.primaryKeys());
		List<String> intersection = new ArrayList<>(sourceJoinKeys);
		intersection.retainAll(primaryKeys);
		return intersection;
	}

	/**
	 * Analyze the parent related table of the current table
	 * If the model field of the parent table contains the source field in the join keys of the current table, it means that the two tables are related.
	 *
	 * @param id The ID of the table that currently needs to be analyzed
	 * @return Analyze result list of {@link JoinKeyReference}
	 */
	private List<JoinKeyReference> analyzeParentReference(String id) {
		MergeTableProperties mergeTableProperties = mergeTablePropertiesMap.get(id);
		if (null == mergeTableProperties) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.ANALYZE_REFERENCE_FAILED_CANT_GET_MERGE_TABLE_PROPERTIES_BY_ID, "Get by id: " + id);
		}
		List<JoinKeyReference> result = new ArrayList<>();
		AtomicReference<String> parentId = new AtomicReference<>(mergeTableProperties.getParentId());
		while (isRunning()) {
			if (StringUtils.isBlank(parentId.get()) || !this.mergeTablePropertiesMap.containsKey(parentId.get())) {
				break;
			}
			MergeTableProperties parentProperty = this.mergeTablePropertiesMap.get(parentId.get());
			JoinKeyReference joinKeyReference = analyzeJoinKeyReference(parentProperty, mergeTableProperties, AnalyzeReferenceType.PARENT);
			if (MapUtils.isNotEmpty(joinKeyReference.referenceJoinKeys)) {
				result.add(joinKeyReference);
				if (Boolean.TRUE.equals(mergeTableProperties.getEnableUpdateJoinKeyValue())) {
					this.enableUpdateJoinKeyMap.computeIfAbsent(parentId.get(), k -> {
						EnableUpdateJoinKey enableUpdateJoinKey = new EnableUpdateJoinKey();
						enableUpdateJoinKey.enableChildren();
						return enableUpdateJoinKey;
					});
					this.enableUpdateJoinKeyMap.computeIfPresent(parentId.get(), (k, v) -> {
						v.enableChildren();
						return v;
					});
				}
			}
			parentId.set(parentProperty.getParentId());
		}
		return result;
	}

	/**
	 * Analyze the children related table of the current table
	 * If the model field of the child table contains the source field in the join keys of the current table, it means that the two tables are related.
	 *
	 * @param id The ID of the table that currently needs to be analyzed
	 * @return Analyze result list of {@link JoinKeyReference}
	 */
	private List<JoinKeyReference> analyzeChildrenReference(String id) {
		List<JoinKeyReference> joinKeyReferences = new ArrayList<>();
		MergeTableProperties mergeTableProperties = mergeTablePropertiesMap.get(id);
		if (null == mergeTableProperties) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.ANALYZE_CHILD_REFERENCE_FAILED_CANT_GET_MERGE_TABLE_PROPERTIES_BY_ID, "Get by id: " + id);
		}
		if (CollectionUtils.isNotEmpty(mergeTableProperties.getChildren())) {
			recursiveAnalyzeChildrenReference(mergeTableProperties, mergeTableProperties.getChildren(), joinKeyReferences);
		}
		return joinKeyReferences;
	}

	private void recursiveAnalyzeChildrenReference(MergeTableProperties parentProperty, List<MergeTableProperties> childProperties, List<JoinKeyReference> joinKeyReferences) {
		if (null == parentProperty || null == childProperties) {
			return;
		}
		for (MergeTableProperties childProperty : childProperties) {
			JoinKeyReference joinKeyReference = analyzeJoinKeyReference(parentProperty, childProperty, AnalyzeReferenceType.CHILDREN);
			if (null == joinKeyReference) {
				joinKeyReference = JoinKeyReference.create(childProperty);
			}
			joinKeyReferences.add(joinKeyReference);
			if (CollectionUtils.isNotEmpty(childProperty.getChildren())) {
				List<JoinKeyReference> childJoinKeyReferences = new ArrayList<>();
				recursiveAnalyzeChildrenReference(parentProperty, childProperty.getChildren(), childJoinKeyReferences);
				joinKeyReference.setChildJoinKeyReferences(childJoinKeyReferences);
			}
		}
	}

	protected JoinKeyReference analyzeJoinKeyReference(MergeTableProperties parentProperty, MergeTableProperties childProperty, AnalyzeReferenceType analyzeReferenceType) {
		TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
		String parentTargetPath = parentProperty.getTargetPath();
		String parentTableName = getTableName(getPreNode(parentProperty.getId()));
		TapTable parentTapTable = tapTableMap.get(parentTableName);
		Set<String> parentFieldNames = parentTapTable.getNameFieldMap().keySet();
		Set<String> parentFieldNameWithTargetPath = parentFieldNames.stream().map(f -> {
			if (StringUtils.isNotBlank(parentTargetPath)) {
				return String.join(".", parentTargetPath, f);
			}
			return f;
		}).collect(Collectors.toSet());

		JoinKeyReference joinKeyReference;
		if (analyzeReferenceType.equals(AnalyzeReferenceType.PARENT)) {
			joinKeyReference = JoinKeyReference.create(parentProperty);
		} else if (analyzeReferenceType.equals(AnalyzeReferenceType.CHILDREN)) {
			joinKeyReference = JoinKeyReference.create(childProperty);
		} else {
			return null;
		}

		List<Map<String, String>> joinKeys = childProperty.getJoinKeys();
		for (Map<String, String> joinKey : joinKeys) {
			String sourceJoinKey = joinKey.get(JoinConditionType.SOURCE.getType());
			String targetJoinKey = joinKey.get(JoinConditionType.TARGET.getType());
			if (parentFieldNameWithTargetPath.contains(targetJoinKey)) {
				String parentFieldName = targetJoinKey;
				if (StringUtils.isNotBlank(parentTargetPath)) {
					parentFieldName = StringUtils.removeStart(targetJoinKey, parentTargetPath + '.');
				}
				if (analyzeReferenceType.equals(AnalyzeReferenceType.PARENT)) {
					joinKeyReference.addJoinKey(sourceJoinKey, parentFieldName);
				} else {
					joinKeyReference.addJoinKey(parentFieldName, sourceJoinKey);
				}
			}
		}

		return joinKeyReference;
	}

	public static class MergeTablePropertyReference {
		private List<JoinKeyReference> parentJoinKeyReferences;
		private List<JoinKeyReference> childJoinKeyReferences;

		public void setParentJoinKeyReferences(List<JoinKeyReference> parentJoinKeyReferences) {
			this.parentJoinKeyReferences = parentJoinKeyReferences;
		}

		public void setChildJoinKeyReferences(List<JoinKeyReference> childJoinKeyReferences) {
			this.childJoinKeyReferences = childJoinKeyReferences;
		}

		public List<JoinKeyReference> getParentJoinKeyReferences() {
			return parentJoinKeyReferences;
		}

		public List<JoinKeyReference> getChildJoinKeyReferences() {
			return childJoinKeyReferences;
		}
	}

	public static class JoinKeyReference {
		private MergeTableProperties mergeTableProperties;
		private Map<String, String> referenceJoinKeys;
		private List<JoinKeyReference> childJoinKeyReferences;

		public JoinKeyReference(MergeTableProperties mergeTableProperties) {
			this.referenceJoinKeys = new HashMap<>();
			this.mergeTableProperties = mergeTableProperties;
		}

		public static JoinKeyReference create(MergeTableProperties mergeTableProperties) {
			return new JoinKeyReference(mergeTableProperties);
		}

		public void addJoinKey(String fieldName, String referenceFieldName) {
			this.referenceJoinKeys.put(fieldName, referenceFieldName);
		}

		public MergeTableProperties getMergeTableProperties() {
			return mergeTableProperties;
		}

		public Map<String, String> getReferenceJoinKeys() {
			return referenceJoinKeys;
		}

		public List<JoinKeyReference> getChildJoinKeyReferences() {
			return childJoinKeyReferences;
		}

		public void setChildJoinKeyReferences(List<JoinKeyReference> childJoinKeyReferences) {
			this.childJoinKeyReferences = childJoinKeyReferences;
		}
	}

	protected enum AnalyzeReferenceType {
		PARENT,
		CHILDREN,
	}

	protected ExternalStorageDto copyExternalStorage(int inMemSize) {
		if (null == externalStorageDto) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.COPY_EXTERNAL_STORAGE_FAILED_SOURCE_IS_NULL);
		}
		ExternalStorageDto externalStorageDtoCopy = new ExternalStorageDto();
		BeanUtils.copyProperties(externalStorageDto, externalStorageDtoCopy);
		externalStorageDtoCopy.setTable(null);
		externalStorageDtoCopy.setInMemSize(inMemSize);
		externalStorageDtoCopy.setWriteDelaySeconds(10);
		externalStorageDtoCopy.setTtlDay(0);
		return externalStorageDtoCopy;
	}

	protected ConstructIMap<Document> buildConstructIMap(HazelcastInstance hazelcastInstance, String referenceId, String cacheName, ExternalStorageDto externalStorageDtoCopy) {
		return new ConstructIMap<>(hazelcastInstance, referenceId, cacheName, externalStorageDtoCopy);
	}

	protected boolean isSourceHaveBefore(String id) {
		if (null == this.sourceConnectionMap) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_SOURCE_CONNECTION_MAP_EMPTY);
		}
		Connections connections = this.sourceConnectionMap.get(id);
		if (null == connections) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_CANT_GET_SOURCE_CONNECTION_BY_ID, "Get by id: " + id);
		}
		List<Capability> capabilities = connections.getCapabilities();
		return CollectionUtils.isNotEmpty(capabilities)
				&& capabilities.stream().anyMatch(cap -> null != cap && ConnectionOptions.CAPABILITY_SOURCE_INCREMENTAL_UPDATE_EVENT_HAVE_BEFORE.equals(cap.getId()));
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
		obsLogger.info(lookupLog.toString());
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

	protected void initSourceConnectionMap(List<MergeTableProperties> mergeTableProperties) {
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

	protected void initShareJoinKeys() {
		this.shareJoinKeysMap = new HashMap<>();
		if (MapUtils.isEmpty(this.mergeTablePropertiesMap)) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.INIT_SHARE_JOIN_KEYS_FAILED_TABLE_MERGE_MAP_EMPTY);
		}
		for (Map.Entry<String, MergeTableProperties> entry : this.mergeTablePropertiesMap.entrySet()) {
			String id = entry.getKey();
			MergeTableProperties mergeTableProperties = entry.getValue();
			if (mergeTableProperties.getMergeType() != MergeTableProperties.MergeType.updateWrite) {
				continue;
			}
			List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
			String targetPath = mergeTableProperties.getTargetPath();
			for (Map<String, String> joinKey : joinKeys) {
				String sourceJoinKey = joinKey.get(JoinConditionType.SOURCE.getType());
				if (StringUtils.isNotBlank(targetPath)) {
					sourceJoinKey = String.join(".", targetPath, sourceJoinKey);
				}
				if (joinKeyExists(sourceJoinKey, JoinConditionType.TARGET)) {
					this.shareJoinKeysMap.computeIfAbsent(id, k -> new HashSet<>()).add(sourceJoinKey);
				}
				this.shareJoinKeysMap.computeIfAbsent(id, k -> new HashSet<>()).add(joinKey.get(JoinConditionType.TARGET.getType()));
			}
		}
	}

	protected boolean joinKeyExists(String joinKey, JoinConditionType joinConditionType) {
		if (StringUtils.isBlank(joinKey)) {
			return false;
		}
		if (null == joinConditionType) {
			return false;
		}
		if (MapUtils.isEmpty(this.mergeTablePropertiesMap)) {
			return false;
		}
		for (MergeTableProperties mergeTableProperties : this.mergeTablePropertiesMap.values()) {
			String targetPath = mergeTableProperties.getTargetPath();
			List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
			if (CollectionUtils.isEmpty(joinKeys)) {
				continue;
			}
			for (Map<String, String> key : joinKeys) {
				String s = key.get(joinConditionType.getType());
				if (JoinConditionType.SOURCE == joinConditionType && StringUtils.isNotBlank(targetPath)) {
					s = String.join(".", targetPath, s);
				}
				if (joinKey.equals(s)) {
					return true;
				}
			}
		}
		return false;
	}

	private static String getMergeCacheName(String nodeId, String tableName) {
		String name;
		if (StringUtils.isBlank(nodeId)) {
			throw new IllegalArgumentException("Get merge node cache name failed, node id is blank");
		}
		if (StringUtils.isBlank(tableName)) {
			name = TAG + "_" + nodeId + "_" + LOOKUP_TABLE_SUFFIX;
		} else {
			name = TAG + "_" + tableName + "_" + nodeId + "_" + LOOKUP_TABLE_SUFFIX;
		}
		return name;
	}

	private static String getCheckUpdateJoinKeyValueCacheName(String nodeId) {
		if (StringUtils.isBlank(nodeId)) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.GET_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_NAME_FAILED_NODE_ID_CANNOT_NULL);
		}
		return String.join("_", TAG, nodeId, UPDATE_JOIN_KEY_VALUE_CACHE_TABLE_SUFFIX);
	}

	private MergeTableProperties getMergeProperty(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) {
			return null;
		}
		String preNodeId = getPreNodeId(tapdataEvent);
		return this.mergeTablePropertiesMap.get(preNodeId);
	}

	private ConstructIMap<Document> getHazelcastConstruct(String sourceNodeId) {
		return this.mergeCacheMap.getOrDefault(sourceNodeId, null);
	}

	protected String getPreNodeId(TapdataEvent tapdataEvent) {
		List<String> nodeIds = tapdataEvent.getNodeIds();
		if (CollectionUtils.isEmpty(nodeIds)) {
			throw new RuntimeException("From node id list is empty, " + tapdataEvent);
		}
		if (nodeIds.size() > 1) {
			return nodeIds.get(nodeIds.size() - 1);
		} else if (nodeIds.size() == 1) {
			return nodeIds.get(0);
		} else {
			return null;
		}
	}

	protected boolean needCache(TapdataEvent tapdataEvent) {
		if (isInitialSyncTask() && !isSubTableFirstMode()) return false;
		if (isInvalidOperation(tapdataEvent)) return false;
		String preNodeId = getPreNodeId(tapdataEvent);
		return needCacheIdList.contains(preNodeId);
	}

	protected boolean needLookup(TapdataEvent tapdataEvent) {
		if (isInitialSyncTask() && !isSubTableFirstMode()) return false;
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

	protected boolean isSubTableFirstMode() {
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

	protected void cache(List<TapdataEvent> tapdataEvents) {
		if (null == tapdataEvents) {
			return;
		}
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
					Throwable matchThrowable = CommonUtils.matchThrowable(e, TapCodeException.class);
					if (null != matchThrowable) {
						throw (TapCodeException) matchThrowable;
					}
					throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHE_UNKNOWN_ERROR, "First event: " + events.get(0), e);
				}
				break;
			case DELETE:
				events.forEach(event -> {
					try {
						deleteCache(event, mergeProperty, hazelcastConstruct);
					} catch (Exception e) {
						Throwable matchThrowable = CommonUtils.matchThrowable(e, TapCodeException.class);
						if (null != matchThrowable) {
							throw (TapCodeException) matchThrowable;
						}
						throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHE_UNKNOWN_ERROR, "Event: " + event, e);
					}
				});
				break;
		}
	}

	protected void upsertCache(TapdataEvent tapdataEvent, MergeTableProperties mergeTableProperty, ConstructIMap<Document> hazelcastConstruct) {
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
		transformDateTime(after);
		groupByJoinKeyValues.put(encodePkOrUniqueValueKey, after);
		try {
			hazelcastConstruct.upsert(encodeJoinValueKey, groupByJoinKeyValues);
		} catch (Exception e) {
			throw new TapEventException(TaskMergeProcessorExCode_16.UPSERT_CACHE_FAILED, String.format("- Construct name: %s\n- Join value key: %s, encode: %s\n- Pk or unique values: %s, encode: %s\n- Find by join value key result: %s",
					hazelcastConstruct.getName(), joinValueKey, encodeJoinValueKey, pkOrUniqueValueKey, encodePkOrUniqueValueKey, groupByJoinKeyValues.toJson()), e).addEvent(tapdataEvent.getTapEvent());
		}
	}

	protected void upsertCache(List<TapdataEvent> tapdataEvents, MergeTableProperties mergeTableProperties, ConstructIMap<Document> hazelcastConstruct) {
		Map<String, List<TapdataEvent>> joinValueKeyTapdataEventMap = new HashMap<>();
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			Map<String, Object> after = getAfter(tapdataEvent);
			String joinValueKeyBySource = getJoinValueKeyBySource(after, mergeTableProperties, hazelcastConstruct);
			String encodeJoinValueKey = encode(joinValueKeyBySource);
			List<TapdataEvent> list = joinValueKeyTapdataEventMap.computeIfAbsent(encodeJoinValueKey, k -> new ArrayList<>());
			list.add(tapdataEvent);
		}
		Map<String, Object> groupByJoinKeyValues;
		try {
			groupByJoinKeyValues = hazelcastConstruct.findAll(joinValueKeyTapdataEventMap.keySet());
		} catch (Exception e) {
			if (e.getCause() instanceof InterruptedException) {
				Thread.currentThread().interrupt();
				return;
			}
			throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHE_FIND_BY_JOIN_KEYS_FAILED, "Find by keys: " + joinValueKeyTapdataEventMap.keySet() + ", first event: " + tapdataEvents.get(0).getTapEvent(), e);
		}
		Map<String, Document> insertMap = new HashMap<>();
		for (String joinValueKey : joinValueKeyTapdataEventMap.keySet()) {
			Object groupByJoinKeyValue = groupByJoinKeyValues.get(joinValueKey);
			if (null == groupByJoinKeyValue) {
				groupByJoinKeyValue = new Document();
			}
			if (groupByJoinKeyValue instanceof Document) {
				List<TapdataEvent> list = joinValueKeyTapdataEventMap.get(joinValueKey);
				for (TapdataEvent tapdataEvent : list) {
					Map<String, Object> after = getAfter(tapdataEvent);
					String pkOrUniqueValueKey = getPkOrUniqueValueKey(after, mergeTableProperties, hazelcastConstruct);
					String encodePkOrUniqueValueKey = encode(pkOrUniqueValueKey);
					transformDateTime(after);
					((Document) groupByJoinKeyValue).put(encodePkOrUniqueValueKey, after);
					insertMap.put(joinValueKey, (Document) groupByJoinKeyValue);
				}
			}
		}
		try {
			hazelcastConstruct.insertMany(insertMap);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.UPSERT_CACHES_FAILED, e);
		}
	}

	protected void transformDateTime(Map<String, Object> after) {
		mapIterator.iterate(after, (key, value, recursive) -> {
			if (value instanceof DateTime) {
				return ((DateTime) value).toDate();
			}
			return value;
		});
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
			if (value instanceof Number) {
				values.add(new BigDecimal(String.valueOf(value)).stripTrailingZeros().toPlainString());
			} else {
				values.add(String.valueOf(value));
			}
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
			if (value instanceof Number) {
				values.add(new BigDecimal(String.valueOf(value)).stripTrailingZeros().toPlainString());
			} else {
				values.add(String.valueOf(value));
			}
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

	protected String getPkOrUniqueValueKey(Map<String, Object> data, MergeTableProperties mergeProperty, ConstructIMap<Document> hazelcastConstruct) {
		String sourceNodeId = mergeProperty.getId();
		return getPkOrUniqueValueKey(data, sourceNodeId, hazelcastConstruct);
	}

	protected String getPkOrUniqueValueKey(Map<String, Object> data, String sourceNodeId, ConstructIMap<Document> hazelcastConstruct) {
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
			if (value instanceof Number) {
				values.add(new BigDecimal(String.valueOf(value)).stripTrailingZeros().toPlainString());
			} else if (null == value) {
				values.add("null");
			} else {
				values.add(String.valueOf(value));
			}
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

	protected String getPreTableName(TapdataEvent tapdataEvent) {
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

	protected List<MergeLookupResult> lookup(TapdataEvent tapdataEvent) {
		List<String> nodeIds = tapdataEvent.getNodeIds();
		if (CollectionUtils.isEmpty(nodeIds)) {
			throw new TapEventException(TaskMergeProcessorExCode_16.LOOK_UP_MISSING_FROM_NODE_ID).addEvent(tapdataEvent.getTapEvent());
		}
		String sourceNodeId = getPreNodeId(tapdataEvent);
		MergeTableProperties currentMergeTableProperty = this.mergeTablePropertiesMap.get(sourceNodeId);
		Map<String, Object> after = getAfter(tapdataEvent);
		List<MergeLookupResult> mergeLookupResults;
		try {
			mergeLookupResults = recursiveLookup(currentMergeTableProperty, after, true);
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
													Map<String, Object> data,
													boolean lookupDataExists) {
		List<MergeTableProperties> children = mergeTableProperties.getChildren();
		if (CollectionUtils.isEmpty(children)) return new ArrayList<>();
		List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
		for (MergeTableProperties childMergeProperty : children) {
			MergeTableProperties.MergeType mergeType = childMergeProperty.getMergeType();
			String joinValueKey = null;
			Document findData = null;
			if (lookupDataExists) {
				ConstructIMap<Document> hazelcastConstruct = getHazelcastConstruct(childMergeProperty.getId());
				joinValueKey = getJoinValueKeyByTarget(data, childMergeProperty, mergeTableProperties, hazelcastConstruct);
				if (joinValueKey == null) {
					continue;
				}
				String encodeJoinValueKey = encode(joinValueKey);
				try {
					findData = hazelcastConstruct.find(encodeJoinValueKey);
					if (nodeLogger.isDebugEnabled()) {
						nodeLogger.debug("Lookup find data filter: {}({}), result: {}", joinValueKey, encodeJoinValueKey, findData);
					}
				} catch (Exception e) {
					throw new TapCodeException(TaskMergeProcessorExCode_16.LOOK_UP_FIND_BY_JOIN_KEY_FAILED, String.format("- Find construct name: %s%n- Join key: %s%n- Encoded join key: %s", hazelcastConstruct.getName(), joinValueKey, encodeJoinValueKey), e);
				}
			}
			io.tapdata.pdk.apis.entity.merge.MergeTableProperties copyMergeTableProperty = copyMergeTableProperty(childMergeProperty);
			boolean mockData = false;
			Node<?> preNode = getPreNode(childMergeProperty.getId());
			String tableName = getTableName(preNode);
			TapTable tapTable = processorBaseContext.getTapTableMap().get(tableName);
			if (MergeTableProperties.MergeType.updateWrite == mergeType) {
				MergeLookupResult mergeLookupResult = new MergeLookupResult();
				Set<String> shareJoinKeys = shareJoinKeysMap.get(copyMergeTableProperty.getId());
				Map<String, Object> lookupData;
				if (MapUtils.isEmpty(findData)) {
					lookupData = mockLookupMap(childMergeProperty);
					mockData = true;
				} else {
					Set<String> keySet = findData.keySet();
					keySet.remove("_ts");
					if (keySet.size() > 1) {
						logger.warn("Update write merge lookup, find more than one row by join key: {}, will use first row: {}", joinValueKey, data);
					}
					if (keySet.isEmpty()) {
						// All cache data in the join key have been deleted
						lookupData = mockLookupMap(childMergeProperty);
						mockData = true;
					} else {
						String firstKey = keySet.iterator().next();
						lookupData = (Map<String, Object>) findData.get(firstKey);
					}
				}

				mergeLookupResult.setSharedJoinKeys(shareJoinKeys);
				mergeLookupResult.setProperty(copyMergeTableProperty);
				mergeLookupResult.setData(lookupData);
				mergeLookupResult.setDataExists(!mockData);
				mergeLookupResult.setTapTable(tapTable);
				mergeLookupResult.setMergeLookupResults(recursiveLookup(childMergeProperty, lookupData, mergeLookupResult.isDataExists()));
				mergeLookupResults.add(mergeLookupResult);
			} else if (MergeTableProperties.MergeType.updateIntoArray == mergeType) {
				Collection<Object> lookupArray;
				if (MapUtils.isEmpty(findData)) {
					Map<String, Object> mockDataMap = mockLookupMap(childMergeProperty);
					lookupArray = Collections.singletonList(mockDataMap);
					mockData = true;
				} else {
					lookupArray = findData.values();
				}
				for (Object arrayData : lookupArray) {
					if (!(arrayData instanceof Map)) continue;
					MergeLookupResult mergeLookupResult = new MergeLookupResult();
					mergeLookupResult.setProperty(copyMergeTableProperty);
					mergeLookupResult.setData((Map<String, Object>) arrayData);
					mergeLookupResult.setDataExists(!mockData);
					mergeLookupResult.setTapTable(tapTable);
					mergeLookupResult.setMergeLookupResults(recursiveLookup(childMergeProperty, (Map<String, Object>) arrayData, true));
					mergeLookupResults.add(mergeLookupResult);
				}
			}
		}
		return mergeLookupResults;
	}

	@NotNull
	private Map<String, Object> mockLookupMap(MergeTableProperties childMergeProperty) {
		Map<String, Object> lookupMap;
		lookupMap = new HashMap<>();
		String tableName = getTableName(getPreNode(childMergeProperty.getId()));
		TapTable tapTable = processorBaseContext.getTapTableMap().get(tableName);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		nameFieldMap.keySet().forEach(key -> lookupMap.put(key, null));
		return lookupMap;
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create();
		dataMap.kv("lookup thread pool size", lookupThreadNum);
		dataMap.kv("lookup runnable queue", null == lookupQueue ? 0 : lookupQueue.size());
		dataMap.kv("last batch metrics", batchProcessMetrics.toString());
		dataMap.kv("share join key", shareJoinKeysMap);
		return dataMap;
	}

	protected enum JoinConditionType {
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

	public static void clearCache(Node<?> node, List<Node> nodes, List<Edge> edges) {
		if (!(node instanceof MergeTableNode)) return;
		ExternalStorageDto externalStorage = ExternalStorageUtil.getTargetNodeExternalStorage(node, edges, ConnectorConstant.clientMongoOperator, nodes);
		recursiveClearCache(externalStorage, ((MergeTableNode) node).getMergeProperties(), HazelcastUtil.getInstance());
	}

	private static void recursiveClearCache(ExternalStorageDto externalStorageDto, List<MergeTableProperties> mergeTableProperties, HazelcastInstance hazelcastInstance) {
		if (CollectionUtils.isEmpty(mergeTableProperties)) return;
		CommonUtils.handleAnyErrors((Consumer<Throwable> consumer) -> {
			for (MergeTableProperties mergeTableProperty : mergeTableProperties) {
				try {
					ConstructIMap<Document> mergeCache = new ConstructIMap<>(hazelcastInstance, HazelcastMergeNode.class.getSimpleName(),
							getMergeCacheName(mergeTableProperty.getId(), mergeTableProperty.getTableName()), externalStorageDto);
					try {
						mergeCache.clear();
						mergeCache.destroy();
					} catch (Exception e) {
						throw new TapCodeException(TaskMergeProcessorExCode_16.CLEAR_AND_DESTROY_CACHE_FAILED, clearAndDestroyCacheErrorMessage(e, mergeCache), e);
					}
					ConstructIMap<Document> updateJoinKeyCache = new ConstructIMap<>(hazelcastInstance, String.join("_", TAG, UPDATE_JOIN_KEY_VALUE_CACHE_TABLE_SUFFIX),
							String.valueOf(getCheckUpdateJoinKeyValueCacheName(mergeTableProperty.getId()).hashCode()), externalStorageDto);
					try {
						updateJoinKeyCache.clear();
						updateJoinKeyCache.destroy();
					} catch (Exception e) {
						throw new TapCodeException(TaskMergeProcessorExCode_16.CLEAR_AND_DESTROY_CACHE_FAILED, clearAndDestroyCacheErrorMessage(e, updateJoinKeyCache), e);
					}
					recursiveClearCache(externalStorageDto, mergeTableProperty.getChildren(), hazelcastInstance);
				} catch (Exception e) {
					consumer.accept(e);
				}
			}
		}, null);
	}

	private static String clearAndDestroyCacheErrorMessage(Exception e, ConstructIMap<Document> cache) throws Exception {
		if (null == cache) {
			return "Clear and destroy cache failed, cache is null";
		}
		String msg = String.format("Clear and destroy cache [%s] failed", cache.getName());
		if (cache.exists("sign")) {
			msg += ", sign: " + cache.find("sign");
		}
		return msg += ", error: " + e.getMessage() + ", stack: " + Log4jUtil.getStackString(e);
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
	protected void doClose() throws TapCodeException {
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
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(handleUpdateJoinKeyThreadPool).ifPresent(ExecutorService::shutdownNow), TAG);
		} finally {
			super.doClose();
		}
	}

	protected static class BatchProcessMetrics {
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

	protected void handleBatchUpdateJoinKey(List<BatchEventWrapper> batchEventWrappers) {
		if (CollectionUtils.isEmpty(batchEventWrappers)) {
			return;
		}
		List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
		for (BatchEventWrapper batchEventWrapper : batchEventWrappers) {
			TapdataEvent tapdataEvent = batchEventWrapper.getTapdataEvent();
			if (null == tapdataEvent) {
				continue;
			}
			String preNodeId = getPreNodeId(tapdataEvent);
			EnableUpdateJoinKey enableUpdateJoinKey = this.enableUpdateJoinKeyMap.get(preNodeId);
			if (null == enableUpdateJoinKey || (!enableUpdateJoinKey.isEnableParent() && !enableUpdateJoinKey.isEnableChildren())) {
				continue;
			}
			completableFutures.add(CompletableFuture.runAsync(() -> handleUpdateJoinKey(tapdataEvent), handleUpdateJoinKeyThreadPool));
		}
		if (CollectionUtils.isNotEmpty(completableFutures)) {
			CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).join();
		}
	}

	protected void handleUpdateJoinKey(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) {
			return;
		}
		if (!tapdataEvent.isDML()) {
			return;
		}
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		String preNodeId = getPreNodeId(tapdataEvent);
		if (!(tapEvent instanceof TapUpdateRecordEvent)) {
			if (checkJoinKeyUpdateCacheMap.containsKey(preNodeId)) {
				if (tapEvent instanceof TapInsertRecordEvent) {
					insertJoinKeyCache(tapdataEvent);
				} else if (tapEvent instanceof TapDeleteRecordEvent) {
					deleteJoinKeyCache(tapdataEvent);
				}
			}
			return;
		}
		EnableUpdateJoinKey enableUpdateJoinKey = this.enableUpdateJoinKeyMap.get(preNodeId);
		if (!enableUpdateJoinKey.isEnableParent() && !enableUpdateJoinKey.isEnableChildren()) {
			return;
		}

		MergeTablePropertyReference mergeTablePropertyReference = this.mergeTablePropertyReferenceMap.get(preNodeId);
		List<JoinKeyReference> parentJoinKeyReferences = mergeTablePropertyReference.getParentJoinKeyReferences();
		List<JoinKeyReference> childJoinKeyReferences = mergeTablePropertyReference.getChildJoinKeyReferences();
		if (CollectionUtils.isEmpty(parentJoinKeyReferences) && CollectionUtils.isEmpty(childJoinKeyReferences)) {
			return;
		}
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (MapUtils.isEmpty(after)) {
			return;
		}
		if (MapUtils.isEmpty(before)) {
			if (isSourceHaveBefore(preNodeId)) {
				Node<?> preNode = getPreNode(preNodeId);
				throw new TapCodeException(TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_SOURCE_MUST_HAVE_BEFORE, "Node name: " + preNode.getName() + ", id: " + preNodeId);
			}
			before = getAndUpdateJoinKeyCache(tapdataEvent);
		}
		if (before.containsKey("_ts")) {
			before.remove("_ts");
		}

		removeMergeCacheIfUpdateJoinKey(tapdataEvent, before);

		if (enableUpdateJoinKey.isEnableParent()) {
			boolean comparedParentJoinKeyValueResult = compareParentJoinKeyValue(tapdataEvent, parentJoinKeyReferences, before, after);
			if (comparedParentJoinKeyValueResult) {
				return;
			}
		}
		if (enableUpdateJoinKey.isEnableChildren()) {
			MergeTableProperties mergeTableProperties = this.mergeTablePropertiesMap.get(preNodeId);
			Map<String, Object> beforeJoinKeys = buildParentBeforeJoinKeyValueMap(mergeTableProperties, before);
			recursiveCompareChildrenJoinKeyValue(tapdataEvent, childJoinKeyReferences, before, after, beforeJoinKeys);
		}
	}

	protected void removeMergeCacheIfUpdateJoinKey(TapdataEvent tapdataEvent, Map<String, Object> before) {
		String preNodeId = getPreNodeId(tapdataEvent);
		MergeTableProperties mergeTableProperties = this.mergeTablePropertiesMap.get(preNodeId);
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		ConstructIMap<Document> lookupCache = getHazelcastConstruct(preNodeId);
		if (null == lookupCache) {
			return;
		}
		String beforeJoinValueKeyBySource = getJoinValueKeyBySource(before, mergeTableProperties, lookupCache);
		String afterJoinValueKeyBySource = getJoinValueKeyBySource(after, mergeTableProperties, lookupCache);
		if (beforeJoinValueKeyBySource.equals(afterJoinValueKeyBySource)) {
			return;
		}
		String pkOrUniqueValueKey = getPkOrUniqueValueKey(before, preNodeId, lookupCache);
		String encodeJoinKey = encode(beforeJoinValueKeyBySource);
		String encodePk = encode(pkOrUniqueValueKey);
		Document beforeDoc;
		try {
			beforeDoc = lookupCache.find(encodeJoinKey);
		} catch (Exception e) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_FIND_CACHE_ERROR, "Construct name: " + lookupCache.getName() + ", join value key: " + beforeJoinValueKeyBySource, e);
		}
		if (null == beforeDoc) {
			return;
		}
		beforeDoc.remove(encodePk);
		deleteOrUpdateMergeCache(beforeDoc, lookupCache, encodeJoinKey, beforeJoinValueKeyBySource);
	}

	private static void deleteOrUpdateMergeCache(Document beforeDoc, ConstructIMap<Document> lookupCache, String encodeJoinKey, String beforeJoinValueKeyBySource) {
		if (MapUtils.isEmpty(beforeDoc) || (beforeDoc.size() == 1 && beforeDoc.containsKey("_ts"))) {
			try {
				lookupCache.delete(encodeJoinKey);
			} catch (Exception e) {
				throw new TapCodeException(TaskMergeProcessorExCode_16.REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_DELETE_CACHE_ERROR, "Construct name: " + lookupCache.getName() + ", join value key: " + beforeJoinValueKeyBySource, e);
			}
		} else {
			try {
				lookupCache.upsert(encodeJoinKey, beforeDoc);
			} catch (Exception e) {
				throw new TapCodeException(TaskMergeProcessorExCode_16.REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_UPDATE_CACHE_ERROR, "Construct name: " + lookupCache.getName() + ", join value key: " + beforeJoinValueKeyBySource, e);
			}
		}
	}

	private boolean compareParentJoinKeyValue(TapdataEvent tapdataEvent, List<JoinKeyReference> joinKeyReferences, Map<String, Object> before, Map<String, Object> after) {
		String preNodeId = getPreNodeId(tapdataEvent);
		for (JoinKeyReference joinKeyReference : joinKeyReferences) {
			Map<String, String> referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			if (MapUtils.isEmpty(referenceJoinKeys)) {
				continue;
			}
			Map<String, Object> beforeReferenceJoinKey = filterByJoinKey(before, referenceJoinKeys.keySet());
			Map<String, Object> afterReferenceJoinKey = filterByJoinKey(after, referenceJoinKeys.keySet());
			if (joinKeyValueNotEquals(beforeReferenceJoinKey, afterReferenceJoinKey)) {
				MergeTableProperties mergeTableProperties = this.mergeTablePropertiesMap.get(preNodeId);
				addUpdateJoinKeyIntoMergeInfo(tapdataEvent, beforeReferenceJoinKey, afterReferenceJoinKey, mergeTableProperties);
				return true;
			}
		}
		return false;
	}

	private void recursiveCompareChildrenJoinKeyValue(TapdataEvent tapdataEvent, List<JoinKeyReference> joinKeyReferences, Map<String, Object> before, Map<String, Object> after, Map<String, Object> parentBeforeJoinKey) {
		if (CollectionUtils.isEmpty(joinKeyReferences)) {
			return;
		}
		for (JoinKeyReference joinKeyReference : joinKeyReferences) {
			MergeTableProperties mergeTableProperties = joinKeyReference.getMergeTableProperties();
			Map<String, String> referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			if (Boolean.TRUE.equals(mergeTableProperties.getEnableUpdateJoinKeyValue())
					&& MapUtils.isNotEmpty(referenceJoinKeys)) {
				Map<String, Object> beforeReferenceJoinKey = filterByJoinKey(before, referenceJoinKeys.keySet());
				Map<String, Object> afterReferenceJoinKey = filterByJoinKey(after, referenceJoinKeys.keySet());
				if (joinKeyValueNotEquals(beforeReferenceJoinKey, afterReferenceJoinKey)) {
					addUpdateJoinKeyIntoMergeInfo(tapdataEvent, beforeReferenceJoinKey, afterReferenceJoinKey, mergeTableProperties, parentBeforeJoinKey);
					continue;
				}
			}
			List<JoinKeyReference> childJoinKeyReferences = joinKeyReference.getChildJoinKeyReferences();
			if (CollectionUtils.isNotEmpty(childJoinKeyReferences)) {
				List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
				Map<String, Object> beforeJoinKey = buildParentBeforeJoinKeyValueMap(mergeTableProperties, before);
				if (MapUtils.isNotEmpty(parentBeforeJoinKey)) {
					beforeJoinKey.putAll(parentBeforeJoinKey);
				}
				recursiveCompareChildrenJoinKeyValue(tapdataEvent, childJoinKeyReferences, before, after, beforeJoinKey);
			}
		}
	}

	protected static Map<String, Object> buildParentBeforeJoinKeyValueMap(MergeTableProperties mergeTableProperties, Map<String, Object> before) {
		Map<String, Object> beforeJoinKey = new HashMap<>();
		List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
		if (CollectionUtils.isEmpty(joinKeys)) {
			return beforeJoinKey;
		}
		for (Map<String, String> joinKey : joinKeys) {
			String targetJoinKey = joinKey.get(JoinConditionType.TARGET.getType());
			String targetJoinKeyWithOutTargetPath = targetJoinKey;
			if (StringUtils.isNotBlank(mergeTableProperties.getTargetPath()) && targetJoinKey.startsWith(mergeTableProperties.getTargetPath() + ".")) {
				targetJoinKeyWithOutTargetPath = StringUtils.removeStart(targetJoinKey, mergeTableProperties.getTargetPath() + ".");
			}
			Object value = MapUtilV2.getValueByKeyV2(before, targetJoinKeyWithOutTargetPath);
			beforeJoinKey.put(targetJoinKey, value);
		}
		return beforeJoinKey;
	}

	private static String mapValues2String(Map<String, Object> beforeJoinKey) {
		return beforeJoinKey.values().stream().map(String::valueOf).collect(Collectors.joining("_"));
	}

	private void addUpdateJoinKeyIntoMergeInfo(TapdataEvent tapdataEvent, Map<String, Object> before, Map<String, Object> after, MergeTableProperties mergeTableProperties) {
		addUpdateJoinKeyIntoMergeInfo(tapdataEvent, before, after, mergeTableProperties, null);
	}

	private void addUpdateJoinKeyIntoMergeInfo(TapdataEvent tapdataEvent, Map<String, Object> before, Map<String, Object> after, MergeTableProperties mergeTableProperties, Map<String, Object> parentBeforeJoinKey) {
		MergeInfo mergeInfo = wrapMergeInfo(tapdataEvent);
		MergeInfo.UpdateJoinKey updateJoinKey = new MergeInfo.UpdateJoinKey(before, after, parentBeforeJoinKey);
		mergeInfo.addUpdateJoinKey(mergeTableProperties.getId(), updateJoinKey);
	}

	protected boolean joinKeyValueNotEquals(Map<String, Object> before, Map<String, Object> after) {
		String beforeJoinKeyString = mapValues2String(before);
		String afterJoinKeyString = mapValues2String(after);
		return !StringUtils.equals(beforeJoinKeyString, afterJoinKeyString);
	}

	protected static Map<String, Object> filterByJoinKey(Map<String, Object> data, Set<String> sourceJoinKeys) {
		Map<String, Object> result = new HashMap<>();
		for (String sourceJoinKey : sourceJoinKeys) {
			Object value = MapUtilV2.getValueByKey(data, sourceJoinKey);
			if (value instanceof NotExistsNode) {
				continue;
			}
			result.put(sourceJoinKey, value);
		}
		return result;
	}

	protected Map<String, Object> getAndUpdateJoinKeyCache(TapdataEvent tapdataEvent) {
		String preNodeId = getPreNodeId(tapdataEvent);
		ConstructIMap<Document> constructIMap = this.checkJoinKeyUpdateCacheMap.get(preNodeId);
		if (null == constructIMap) {
			Node<?> preNode = getPreNode(preNodeId);
			throw new TapCodeException(TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP, "Node name: " + preNode.getName() + ", id: " + preNodeId);
		}
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (MapUtils.isEmpty(after)) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_AFTER_IS_EMPTY, "Tap event: " + tapEvent);
		}
		transformDateTime(after);
		String pkOrUniqueValueKey = getPkOrUniqueValueKey(after, preNodeId, constructIMap);
		Document beforeDoc;
		try {
			beforeDoc = constructIMap.find(pkOrUniqueValueKey);
		} catch (Exception e) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_FIND_BY_PK_FAILED, "Construct name: " + constructIMap.getName() + ", pk or unique value key: " + pkOrUniqueValueKey, e);
		}
		if (null == beforeDoc) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_FIND_BEFORE, String.format("Construct name: %s, after: %s, filter value: %s", constructIMap.getName(), after, pkOrUniqueValueKey));
		}
		Map<String, Object> before = new HashMap<>(beforeDoc);
		Document afterDoc = new Document(after);
		try {
			constructIMap.upsert(pkOrUniqueValueKey, afterDoc);
		} catch (Exception e) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED, "Construct name: " + constructIMap.getName() + ", pk or unique value key: " + pkOrUniqueValueKey + ", after: " + afterDoc.toJson(), e);
		}
		return before;
	}

	protected void insertJoinKeyCache(TapdataEvent tapdataEvent) {
		String preNodeId = getPreNodeId(tapdataEvent);
		ConstructIMap<Document> constructIMap = this.checkJoinKeyUpdateCacheMap.get(preNodeId);
		if (null == constructIMap) {
			Node<?> preNode = getPreNode(preNodeId);
			throw new TapCodeException(TaskMergeProcessorExCode_16.INSERT_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP, "Node name: " + preNode.getName() + ", id: " + preNodeId);
		}
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		transformDateTime(after);
		String pkOrUniqueValueKey = getPkOrUniqueValueKey(after, preNodeId, constructIMap);
		Document afterDoc = new Document(after);
		try {
			constructIMap.insert(pkOrUniqueValueKey, afterDoc);
		} catch (Exception e) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.INSERT_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED, "Construct name: " + constructIMap.getName() + ", pk or unique value key: " + pkOrUniqueValueKey + ", after: " + afterDoc.toJson(), e);
		}
	}

	protected void deleteJoinKeyCache(TapdataEvent tapdataEvent) {
		String preNodeId = getPreNodeId(tapdataEvent);
		ConstructIMap<Document> constructIMap = this.checkJoinKeyUpdateCacheMap.get(preNodeId);
		if (null == constructIMap) {
			Node<?> preNode = getPreNode(preNodeId);
			throw new TapCodeException(TaskMergeProcessorExCode_16.DELETE_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP, "Node name: " + preNode.getName() + ", id: " + preNodeId);
		}
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		transformDateTime(before);
		String pkOrUniqueValueKey = getPkOrUniqueValueKey(before, preNodeId, constructIMap);
		if (StringUtils.isBlank(pkOrUniqueValueKey)) {
			return;
		}
		try {
			constructIMap.delete(pkOrUniqueValueKey);
		} catch (Exception e) {
			throw new TapCodeException(TaskMergeProcessorExCode_16.DELETE_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED, "Construct name: " + constructIMap.getName() + ", pk or unique value key: " + pkOrUniqueValueKey, e);
		}
	}

	protected static class EnableUpdateJoinKey {
		private boolean enableParent = false;
		private boolean enableChildren = false;

		public void enableParent() {
			this.enableParent = true;
		}

		public void enableChildren() {
			this.enableChildren = true;
		}

		public boolean isEnableParent() {
			return enableParent;
		}

		public boolean isEnableChildren() {
			return enableChildren;
		}
	}

	@Override
	protected void transformToTapValue(TapdataEvent tapdataEvent, TapTableMap<String, TapTable> tapTableMap, String tableName, TapValueTransform tapValueTransform) {
		super.transformToTapValue(tapdataEvent, tapTableMap, tableName, tapValueTransform);
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		Object mergeInfoObj = tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY);
		if (mergeInfoObj instanceof MergeInfo) {
			MergeInfo mergeInfo = (MergeInfo) mergeInfoObj;
			List<MergeLookupResult> mergeLookupResults = mergeInfo.getMergeLookupResults();
			recursiveMergeInfoTransformToTapValue(mergeLookupResults);
		}
	}

	protected void recursiveMergeInfoTransformToTapValue(List<MergeLookupResult> mergeLookupResults) {
		if (CollectionUtils.isEmpty(mergeLookupResults)) return;
		TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
		for (MergeLookupResult mergeLookupResult : mergeLookupResults) {
			Map<String, Object> data = mergeLookupResult.getData();
			String id = mergeLookupResult.getProperty().getId();
			Node<?> preNode = getPreNode(id);
			String tableName = getTableName(preNode);
			LinkedHashMap<String, TapField> nameFieldMap = tapTableMap.get(tableName).getNameFieldMap();
			if (MapUtils.isNotEmpty(data)) {
				mapIterator.iterate(data, (key, value, recursive) -> {
					if (null == nameFieldMap) {
						return value;
					}
					TapField tapField = nameFieldMap.get(key);
					if (null != tapField
							&& BsonType.OBJECT_ID.name().equals(tapField.getDataType())
							&& value instanceof String
					) {
						return new TapStringValue(value.toString())
								.tapType(new TapString(24L, true))
								.originType(BsonType.OBJECT_ID.name());
					}
					return value;
				});
			}
			List<MergeLookupResult> childMergeLookupResults = mergeLookupResult.getMergeLookupResults();
			if (CollectionUtils.isNotEmpty(childMergeLookupResults)) {
				recursiveMergeInfoTransformToTapValue(childMergeLookupResults);
			}
		}
	}
}

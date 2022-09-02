package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hazelcast.jet.core.Inbox;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageResult;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskMilestoneFuncAspect;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.ResetCounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.PartitionConcurrentProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.KeysPartitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.TapEventPartitionKeySelector;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.metrics.TaskSampleRetriever;
import io.tapdata.milestone.MilestoneContext;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:58
 **/
public abstract class HazelcastTargetPdkBaseNode extends HazelcastPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkBaseNode.class);
	protected Map<String, SyncProgress> syncProgressMap = new ConcurrentHashMap<>();
	protected Map<String, String> tableNameMap;
	protected String tableName;
	private AtomicBoolean firstBatchEvent = new AtomicBoolean();
	private AtomicBoolean firstStreamEvent = new AtomicBoolean();
	protected List<String> updateConditionFields;
	protected String writeStrategy = "updateOrInsert";
	private AtomicBoolean flushOffset = new AtomicBoolean(false);
	protected ResetCounterSampler resetInputCounter;
	protected CounterSampler inputCounter;
	protected ResetCounterSampler resetInsertedCounter;
	protected CounterSampler insertedCounter;
	protected ResetCounterSampler resetUpdatedCounter;
	protected CounterSampler updatedCounter;
	protected ResetCounterSampler resetDeletedCounter;
	protected CounterSampler deletedCounter;
	protected SpeedSampler inputQPS;
	protected AverageSampler timeCostAvg;
	protected AtomicBoolean uploadDagService;
	private List<MetadataInstancesDto> insertMetadata;
	private Map<String, MetadataInstancesDto> updateMetadata;
	private List<String> removeMetadata;
	private boolean initialConcurrent;
	private int initialConcurrentWriteNum;
	private boolean cdcConcurrent;
	private int cdcConcurrentWriteNum;

	private PartitionConcurrentProcessor initialPartitionConcurrentProcessor;
	private PartitionConcurrentProcessor cdcPartitionConcurrentProcessor;
	private boolean inCdc = false;

	public HazelcastTargetPdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		initMilestoneService(MilestoneContext.VertexType.DEST);
		// MILESTONE-INIT_TRANSFORMER-RUNNING
		TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.RUNNING);
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.RUNNING);
	}

	@Override
	protected void initSampleCollector() {
		super.initSampleCollector();

		// init statistic and sample related initialize
		// TODO: init outputCounter initial value
		Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList("inputTotal", "insertedTotal", "updatedTotal", "deletedTotal"));
		resetInputCounter = statisticCollector.getResetCounterSampler("inputTotal");
		inputCounter = sampleCollector.getCounterSampler("inputTotal");
		inputCounter.inc(values.getOrDefault("inputTotal", 0).longValue());
		resetInsertedCounter = statisticCollector.getResetCounterSampler("insertedTotal");
		insertedCounter = sampleCollector.getCounterSampler("insertedTotal");
		insertedCounter.inc(values.getOrDefault("insertedTotal", 0).longValue());
		resetUpdatedCounter = statisticCollector.getResetCounterSampler("updatedTotal");
		updatedCounter = sampleCollector.getCounterSampler("updatedTotal");
		updatedCounter.inc(values.getOrDefault("updatedTotal", 0).longValue());
		resetDeletedCounter = statisticCollector.getResetCounterSampler("deletedTotal");
		deletedCounter = sampleCollector.getCounterSampler("deletedTotal");
		deletedCounter.inc(values.getOrDefault("deletedTotal", 0).longValue());
		inputQPS = sampleCollector.getSpeedSampler("inputQPS");
		timeCostAvg = sampleCollector.getAverageSampler("timeCostAvg");

		statisticCollector.addSampler("replicateLag", () -> {
			Long ts = null;
			for (SyncProgress progress : syncProgressMap.values()) {
				if (null == progress.getSourceTime()) continue;
				if (ts == null || ts > progress.getSourceTime()) {
					ts = progress.getSourceTime();
				}
			}

			return ts == null ? 0 : System.currentTimeMillis() - ts;
		});
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		if (getNode() instanceof TableNode || getNode() instanceof DatabaseNode) {
			try {
				createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
				connectorNodeInit(dataProcessorContext);
			} catch (Throwable e) {
				TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.ERROR, logger);
				MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
				throw new NodeException(e).context(getProcessorBaseContext());
			}
		}
		this.uploadDagService = new AtomicBoolean(false);
		this.insertMetadata = new CopyOnWriteArrayList<>();
		this.updateMetadata = new ConcurrentHashMap<>();
		this.removeMetadata = new CopyOnWriteArrayList<>();

		final Node<?> node = this.dataProcessorContext.getNode();
		if (node instanceof DataParentNode) {
			DataParentNode dataParentNode = (DataParentNode) node;
			final Boolean initialConcurrent = dataParentNode.getInitialConcurrent();
			if (initialConcurrent != null) {
				this.initialConcurrent = initialConcurrent;
				this.initialConcurrentWriteNum = dataParentNode.getInitialConcurrentWriteNum() != null ? dataParentNode.getInitialConcurrentWriteNum() : 8;
				if (initialConcurrent) {
					this.initialPartitionConcurrentProcessor = initConcurrentProcessor(initialConcurrentWriteNum);
					this.initialPartitionConcurrentProcessor.start();
				}
			}
			final Boolean cdcConcurrent = dataParentNode.getCdcConcurrent();
			if (cdcConcurrent != null) {
				this.cdcConcurrent = cdcConcurrent;
				this.cdcConcurrentWriteNum = dataParentNode.getCdcConcurrentWriteNum() != null ? dataParentNode.getCdcConcurrentWriteNum() : 4;
				if (cdcConcurrent) {
					this.cdcPartitionConcurrentProcessor = initConcurrentProcessor(cdcConcurrentWriteNum);
					this.cdcPartitionConcurrentProcessor.start();
				}
			}
		}
	}

	@Override
	final public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
			if (!inbox.isEmpty()) {
				while (isRunning()) {
					List<TapdataEvent> tapdataEvents = new ArrayList<>();
					final int count = inbox.drainTo(tapdataEvents, dataProcessorContext.getTaskDto().getReadBatchSize());
					if (count > 0) {
						if (!inCdc) {
							List<TapdataEvent> partialCdcEvents = new ArrayList<>();
							final Iterator<TapdataEvent> iterator = tapdataEvents.iterator();
							while (iterator.hasNext()) {
								final TapdataEvent tapdataEvent = iterator.next();
								if (tapdataEvent instanceof TapdataStartCdcEvent || inCdc) {
									inCdc = true;
									partialCdcEvents.add(tapdataEvent);
									iterator.remove();
								}
							}

							// initial events and cdc events both in the queue
							if (CollectionUtils.isNotEmpty(partialCdcEvents)) {
								initialProcessEvents(tapdataEvents, false);
								// process partial cdc event
								if (this.initialPartitionConcurrentProcessor != null) {
									this.initialPartitionConcurrentProcessor.stop();
								}
								cdcProcessEvents(partialCdcEvents);
							} else {
								initialProcessEvents(tapdataEvents, true);
							}
						} else {
							cdcProcessEvents(tapdataEvents);
						}
					} else {
						break;
					}
				}
			}
		} catch (Throwable e) {
			errorHandle(e, "Target process failed " + e.getMessage());
			throw sneakyThrow(e);
		} finally {
			ThreadContext.clearAll();
		}
	}

	private void initialProcessEvents(List<TapdataEvent> initialEvents, boolean async) {

		if (CollectionUtils.isNotEmpty(initialEvents)) {
			if (initialConcurrent) {
				this.initialPartitionConcurrentProcessor.process(initialEvents, async);
			} else {
				this.handleTapdataEvents(initialEvents);
			}
		}
	}

	private void cdcProcessEvents(List<TapdataEvent> cdcEvents) {

		if (CollectionUtils.isNotEmpty(cdcEvents)) {
			if (cdcConcurrent) {
				this.cdcPartitionConcurrentProcessor.process(cdcEvents, true);
			} else {
				this.handleTapdataEvents(cdcEvents);
			}
		}
	}

	private void handleTapdataEvents(List<TapdataEvent> tapdataEvents) {
		List<TapEvent> tapEvents = new ArrayList<>();
		List<TapdataShareLogEvent> tapdataShareLogEvents = new ArrayList<>();
		if (null != getConnectorNode()) {
			codecsFilterManager = getConnectorNode().getCodecsFilterManager();
		}
		AtomicReference<TapdataEvent> lastDmlTapdataEvent = new AtomicReference<>();
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			try {
				SyncStage syncStage = tapdataEvent.getSyncStage();
				if (null != syncStage) {
					if (syncStage == SyncStage.INITIAL_SYNC && firstBatchEvent.compareAndSet(false, true)) {
						// MILESTONE-WRITE_SNAPSHOT-RUNNING
						TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.WRITE_SNAPSHOT, MilestoneStatus.RUNNING);
						MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_SNAPSHOT, MilestoneStatus.RUNNING);
					} else if (syncStage == SyncStage.CDC && firstStreamEvent.compareAndSet(false, true)) {
						// MILESTONE-WRITE_CDC_EVENT-FINISH
						TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.WRITE_CDC_EVENT, MilestoneStatus.FINISH);
						MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_CDC_EVENT, MilestoneStatus.FINISH);
					}
				}
				if (tapdataEvent instanceof TapdataHeartbeatEvent) {
					handleTapdataHeartbeatEvent(tapdataEvent);
				} else if (tapdataEvent instanceof TapdataCompleteSnapshotEvent) {
					handleTapdataCompleteSnapshotEvent();
				} else if (tapdataEvent instanceof TapdataStartCdcEvent) {
					handleTapdataStartCdcEvent(tapdataEvent);
				} else if (tapdataEvent instanceof TapdataShareLogEvent) {
					handleTapdataShareLogEvent(tapdataShareLogEvents, tapdataEvent, lastDmlTapdataEvent::set);
				} else {
					if (tapdataEvent.isDML()) {
						handleTapdataRecordEvent(tapdataEvent, tapEvents, lastDmlTapdataEvent::set);
					} else if (tapdataEvent.isDDL()) {
						handleTapdataDDLEvent(tapdataEvent, tapEvents, lastDmlTapdataEvent::set);
					} else {
						if (null != tapdataEvent.getTapEvent()) {
							logger.warn("Tap event type does not supported: " + tapdataEvent.getTapEvent().getClass() + ", will ignore it");
							obsLogger.warn("Tap event type does not supported: " + tapdataEvent.getTapEvent().getClass() + ", will ignore it");
						}
					}
				}
			} catch (Throwable throwable) {
				throw errorHandle(throwable, "handel events failed: " + throwable.getMessage());
			}
		}
		if (CollectionUtils.isNotEmpty(tapEvents)) {
			resetInputCounter.inc(tapEvents.size());
			inputCounter.inc(tapEvents.size());
			inputQPS.add(tapEvents.size());
			try {
				processEvents(tapEvents);
			} catch (Throwable throwable) {
				throw errorHandle(throwable, "process events failed: " + throwable.getMessage());
			}
		}
		if (CollectionUtils.isNotEmpty(tapdataShareLogEvents)) {
			resetInputCounter.inc(tapdataShareLogEvents.size());
			inputCounter.inc(tapdataShareLogEvents.size());
			inputQPS.add(tapdataShareLogEvents.size());
			try {
				processShareLog(tapdataShareLogEvents);
			} catch (Throwable throwable) {
				throw errorHandle(throwable, "process share log failed: " + throwable.getMessage());
			}
		}
		flushSyncProgressMap(lastDmlTapdataEvent.get());
	}

	private void handleTapdataShareLogEvent(List<TapdataShareLogEvent> tapdataShareLogEvents, TapdataEvent tapdataEvent, Consumer<TapdataEvent> consumer) {
		tapdataShareLogEvents.add((TapdataShareLogEvent) tapdataEvent);
		if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
			consumer.accept(tapdataEvent);
		}
	}

	private void handleTapdataStartCdcEvent(TapdataEvent tapdataEvent) {
		// MILESTONE-WRITE_CDC_EVENT-RUNNING
		TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.WRITE_CDC_EVENT, MilestoneStatus.RUNNING);
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_CDC_EVENT, MilestoneStatus.RUNNING);
		flushSyncProgressMap(tapdataEvent);
		saveToSnapshot();
	}

	protected void handleTapdataCompleteSnapshotEvent() {
		// MILESTONE-WRITE_SNAPSHOT-FINISH
		TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.WRITE_SNAPSHOT, MilestoneStatus.FINISH);
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_SNAPSHOT, MilestoneStatus.FINISH);
	}

	private void handleTapdataHeartbeatEvent(TapdataEvent tapdataEvent) {
		flushSyncProgressMap(tapdataEvent);
		saveToSnapshot();
	}

	private void handleTapdataRecordEvent(TapdataEvent tapdataEvent, List<TapEvent> tapEvents, Consumer<TapdataEvent> consumer) {
		TapRecordEvent tapRecordEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
		if (writeStrategy.equals(MergeTableProperties.MergeType.appendWrite.name())) {
			if (!(tapRecordEvent instanceof TapInsertRecordEvent)) {
				return;
			}
		}
		fromTapValue(TapEventUtil.getBefore(tapRecordEvent), codecsFilterManager);
		fromTapValue(TapEventUtil.getAfter(tapRecordEvent), codecsFilterManager);
		tapEvents.add(tapRecordEvent);
		if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
			consumer.accept(tapdataEvent);
		}
	}

	private void handleTapdataDDLEvent(TapdataEvent tapdataEvent, List<TapEvent> tapEvents, Consumer<TapdataEvent> consumer) {
		TapDDLEvent tapDDLEvent = (TapDDLEvent) tapdataEvent.getTapEvent();
		if (tapdataEvent.getTapEvent() instanceof TapCreateTableEvent) {
			updateNode(tapdataEvent);
		}
		updateMemoryFromDDLInfoMap(tapdataEvent, getTgtTableNameFromTapEvent(tapDDLEvent));
		Object updateMetadata = tapDDLEvent.getInfo(UPDATE_METADATA_INFO_KEY);
		if (updateMetadata instanceof Map && MapUtils.isNotEmpty((Map<?, ?>) updateMetadata)) {
			this.updateMetadata.putAll((Map<? extends String, ? extends MetadataInstancesDto>) updateMetadata);
		}
		Object insertMetadata = tapDDLEvent.getInfo(INSERT_METADATA_INFO_KEY);
		if (insertMetadata instanceof List && CollectionUtils.isNotEmpty((Collection<?>) insertMetadata)) {
			this.insertMetadata.addAll((Collection<? extends MetadataInstancesDto>) insertMetadata);
		}
		Object removeMetadata = tapDDLEvent.getInfo(REMOVE_METADATA_INFO_KEY);
		if (removeMetadata instanceof List && CollectionUtils.isNotEmpty((Collection<?>) removeMetadata)) {
			this.removeMetadata.addAll((Collection<? extends String>) removeMetadata);
		}
		tapEvents.add(tapDDLEvent);
		if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
			consumer.accept(tapdataEvent);
		}
	}

	private void flushSyncProgressMap(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) return;
		Node<?> node = processorBaseContext.getNode();
		if (CollectionUtils.isEmpty(tapdataEvent.getNodeIds())) return;
		String progressKey = tapdataEvent.getNodeIds().get(0) + "," + node.getId();
		SyncProgress syncProgress = this.syncProgressMap.get(progressKey);
		if (null == syncProgress) {
			syncProgress = new SyncProgress();
			this.syncProgressMap.put(progressKey, syncProgress);
		}
		if (tapdataEvent instanceof TapdataStartCdcEvent) {
			if (null == tapdataEvent.getSyncStage()) return;
			syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
		} else if (tapdataEvent instanceof TapdataHeartbeatEvent) {
			if (null != tapdataEvent.getStreamOffset()) {
				syncProgress.setStreamOffsetObj(tapdataEvent.getStreamOffset());
			}
			flushOffset.set(true);
		} else {
			if (null == tapdataEvent.getSyncStage()) return;
			if (null == tapdataEvent.getBatchOffset() && null == tapdataEvent.getStreamOffset()) return;
			if (SyncStage.CDC == tapdataEvent.getSyncStage() && null == tapdataEvent.getSourceTime()) return;
			if (null != tapdataEvent.getBatchOffset()) {
				syncProgress.setBatchOffsetObj(tapdataEvent.getBatchOffset());
			}
			if (null != tapdataEvent.getStreamOffset()) {
				syncProgress.setStreamOffsetObj(tapdataEvent.getStreamOffset());
			}
			if (null != tapdataEvent.getSyncStage() && null != syncProgress.getSyncStage() && !syncProgress.getSyncStage().equals(SyncStage.CDC.name())) {
				syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
			}
			syncProgress.setSourceTime(tapdataEvent.getSourceTime());
			if (tapdataEvent.getTapEvent() instanceof TapRecordEvent) {
				syncProgress.setEventTime(((TapRecordEvent) tapdataEvent.getTapEvent()).getReferenceTime());
			}
			syncProgress.setType(tapdataEvent.getType());
			flushOffset.set(true);
		}
		syncProgress.setEventSerialNo(syncProgress.addAndGetSerialNo(1));
	}

	abstract void processEvents(List<TapEvent> tapEvents);

	abstract void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents);

	protected String getTgtTableNameFromTapEvent(TapEvent tapEvent) {
		if (StringUtils.isNotBlank(tableName)) {
			return tableName;
		} else {
			return super.getTgtTableNameFromTapEvent(tapEvent);
		}
	}

	protected void handleTapTablePrimaryKeys(TapTable tapTable) {
		if (writeStrategy.equals(com.tapdata.tm.commons.task.dto.MergeTableProperties.MergeType.updateOrInsert.name())) {
			if (CollectionUtils.isNotEmpty(updateConditionFields)) {
				// 设置逻辑主键
				tapTable.setLogicPrimaries(updateConditionFields);
			} else {
				Collection<String> logicUniqueKey = tapTable.primaryKeys(true);
				if (CollectionUtils.isEmpty(logicUniqueKey)) {
					tapTable.setLogicPrimaries(tapTable.getNameFieldMap().keySet());
				}
			}
		} else if (writeStrategy.equals(com.tapdata.tm.commons.task.dto.MergeTableProperties.MergeType.appendWrite.name())) {
			// 没有关联条件，清空主键信息
			tapTable.getNameFieldMap().values().forEach(v -> v.setPrimaryKeyPos(0));
			tapTable.setLogicPrimaries(null);
		}
	}

	@Override
	public boolean saveToSnapshot() {
		try {
			Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
			if (!flushOffset.get()) return true;
			if (MapUtils.isEmpty(syncProgressMap)) return true;
			Map<String, String> syncProgressJsonMap = new HashMap<>(syncProgressMap.size());
			for (Map.Entry<String, SyncProgress> entry : syncProgressMap.entrySet()) {
				String key = entry.getKey();
				SyncProgress syncProgress = entry.getValue();
				List<String> list = Arrays.asList(key.split(","));
				if (null != syncProgress.getBatchOffsetObj()) {
					syncProgress.setBatchOffset(PdkUtil.encodeOffset(syncProgress.getBatchOffsetObj()));
				}
				if (null != syncProgress.getStreamOffsetObj()) {
					syncProgress.setStreamOffset(PdkUtil.encodeOffset(syncProgress.getStreamOffsetObj()));
				}
				try {
					syncProgressJsonMap.put(JSONUtil.obj2Json(list), JSONUtil.obj2Json(syncProgress));
				} catch (JsonProcessingException e) {
					throw new RuntimeException("Convert offset to json failed, errors: " + e.getMessage(), e);
				}
			}
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			String collection = ConnectorConstant.TASK_COLLECTION + "/syncProgress/" + taskDto.getId();
			try {
				clientMongoOperator.insertOne(syncProgressJsonMap, collection);
			} catch (Exception e) {
				throw new RuntimeException("Save to snapshot failed, collection: " + collection + ", object: " + this.syncProgressMap + "errors: " + e.getMessage(), e);
			}
			if (uploadDagService.get()) {
				// Upload DAG
				TaskDto updateTaskDto = new TaskDto();
				updateTaskDto.setId(taskDto.getId());
				updateTaskDto.setDag(taskDto.getDag());
				clientMongoOperator.insertOne(updateTaskDto, ConnectorConstant.TASK_COLLECTION + "/dag");
				if (MapUtils.isNotEmpty(updateMetadata) || CollectionUtils.isNotEmpty(insertMetadata) || CollectionUtils.isNotEmpty(removeMetadata)) {
					// Upload Metadata
					TransformerWsMessageResult wsMessageResult = new TransformerWsMessageResult();
					wsMessageResult.setBatchInsertMetaDataList(insertMetadata);
					wsMessageResult.setBatchMetadataUpdateMap(updateMetadata);
					wsMessageResult.setBatchRemoveMetaDataList(removeMetadata);
					wsMessageResult.setTaskId(taskDto.getId().toHexString());
					wsMessageResult.setTransformSchema(new HashMap<>());
					// 返回结果调用接口返回
					clientMongoOperator.insertOne(wsMessageResult, ConnectorConstant.TASK_COLLECTION + "/transformer/resultWithHistory");
					insertMetadata.clear();
					updateMetadata.clear();
					removeMetadata.clear();
				}
				uploadDagService.compareAndSet(true, false);
			}
		} finally {
			ThreadContext.clearAll();
		}
		return true;
	}

	@NotNull
	private PartitionConcurrentProcessor initConcurrentProcessor(int cdcConcurrentWriteNum) {
		return new PartitionConcurrentProcessor(
				cdcConcurrentWriteNum,
				dataProcessorContext.getTaskDto().getReadBatchSize(),
				new KeysPartitioner(),
				new TapEventPartitionKeySelector(tapEvent -> {
					final String tgtTableName = getTgtTableNameFromTapEvent(tapEvent);
					TapTable tapTable = dataProcessorContext.getTapTableMap().get(tgtTableName);
					handleTapTablePrimaryKeys(tapTable);
					return new ArrayList<>(tapTable.primaryKeys(true));
				}),
				this::handleTapdataEvents,
				this::flushSyncProgressMap,
				this::errorHandle,
				this::isRunning,
				dataProcessorContext.getTaskDto()
		);
	}

	@Override
	public void doClose() throws Exception {
		try {
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.initialPartitionConcurrentProcessor).ifPresent(PartitionConcurrentProcessor::forceStop), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.cdcPartitionConcurrentProcessor).ifPresent(PartitionConcurrentProcessor::forceStop), TAG);
		} finally {
			super.doClose();
		}
	}
}

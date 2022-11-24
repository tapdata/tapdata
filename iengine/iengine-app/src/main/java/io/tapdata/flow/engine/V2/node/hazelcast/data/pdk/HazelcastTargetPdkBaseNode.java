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
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.PartitionConcurrentProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.KeysPartitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.TapEventPartitionKeySelector;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:58
 **/
public abstract class HazelcastTargetPdkBaseNode extends HazelcastPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	public static final int DEFAULT_TARGET_BATCH_INTERVAL_MS = 1000;
	public static final int DEFAULT_TARGET_BATCH = 500;
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkBaseNode.class);
	protected Map<String, SyncProgress> syncProgressMap = new ConcurrentHashMap<>();
	protected String tableName;
	private AtomicBoolean firstBatchEvent = new AtomicBoolean();
	private AtomicBoolean firstStreamEvent = new AtomicBoolean();
	protected List<String> updateConditionFields;
	protected String writeStrategy = "updateOrInsert";
	private AtomicBoolean flushOffset = new AtomicBoolean(false);
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
	private LinkedBlockingQueue<TapdataEvent> tapEventQueue;
	private final ExecutorService queueConsumerThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), r -> {
		Thread thread = new Thread(r);
		thread.setName(String.format("Target-Queue-Consumer-%s[%s]", getNode().getName(), getNode().getId()));
		return thread;
	});
	private boolean inCdc = false;
	private int targetBatch;
	private long targetBatchIntervalMs;

	public HazelcastTargetPdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		initMilestoneService(MilestoneContext.VertexType.DEST);
		// MILESTONE-INIT_TRANSFORMER-RUNNING
		TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.RUNNING);
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.RUNNING);
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

		this.targetBatch = Math.max(dataProcessorContext.getTaskDto().getWriteBatchSize(), DEFAULT_TARGET_BATCH);
		this.targetBatchIntervalMs = Math.max(dataProcessorContext.getTaskDto().getWriteBatchWaitMs(), DEFAULT_TARGET_BATCH_INTERVAL_MS);
		logger.info("Target node {}[{}] batch size: {}", getNode().getName(), getNode().getId(), targetBatch);
		obsLogger.info("Target node {}[{}] batch size: {}", getNode().getName(), getNode().getId(), targetBatch);
		logger.info("Target node {}[{}] batch max wait interval ms: {}", getNode().getName(), getNode().getId(), targetBatchIntervalMs);
		obsLogger.info("Target node {}[{}] batch max wait interval ms: {}", getNode().getName(), getNode().getId(), targetBatchIntervalMs);
		this.tapEventQueue = new LinkedBlockingQueue<>(targetBatch * 2);
		logger.info("Init target queue complete, size: {}", (targetBatch * 2));
		obsLogger.info("Init target queue complete, size: {}", (targetBatch * 2));
		this.queueConsumerThreadPool.submit(this::queueConsume);
		logger.info("Init target queue consumer complete");

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

		TaskDto taskDto = dataProcessorContext.getTaskDto();
		String type = taskDto.getType();
		if (TaskDto.TYPE_INITIAL_SYNC.equals(type)) {
			putInGlobalMap(getCompletedInitialKey(), false);
		}
		obsLogger.info("Init target queue consumer complete");
	}

	@Override
	final public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
			Thread.currentThread().setName(String.format("Target-Process-%s[%s]", getNode().getName(), getNode().getId()));
			if (!inbox.isEmpty()) {
				List<TapdataEvent> tapdataEvents = new ArrayList<>();
				final int count = inbox.drainTo(tapdataEvents, targetBatch);
				if (count > 0) {
					for (TapdataEvent tapdataEvent : tapdataEvents) {
						while (isRunning()) {
							try {
								if (tapEventQueue.offer(tapdataEvent, 1L, TimeUnit.SECONDS)) {
									break;
								}
							} catch (InterruptedException ignored) {
							}
						}
					}
				}
			}
		} catch (Throwable e) {
			throw new RuntimeException(String.format("Drain from inbox failed: %s", e.getMessage()), e);
		} finally {
			ThreadContext.clearAll();
		}
	}

	private void dispatchTapdataEvents(List<TapdataEvent> tapdataEvents, Consumer<List<TapdataEvent>> consumer) {
		if (null == tapdataEvents || null == consumer) return;
		String preClassName = "";
		List<TapdataEvent> consumeTapdataEvents = new ArrayList<>();
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			if (null == tapdataEvent) continue;
			String currClassName = tapdataEvent.getClass().getName();
			if (!"".equals(preClassName) && !preClassName.equals(currClassName)) {
				consumer.accept(consumeTapdataEvents);
				consumeTapdataEvents.clear();
			}
			preClassName = currClassName;
			consumeTapdataEvents.add(tapdataEvent);
		}
		if (CollectionUtils.isNotEmpty(consumeTapdataEvents)) {
			consumer.accept(consumeTapdataEvents);
			consumeTapdataEvents.clear();
		}
	}

	private void processTargetEvents(List<TapdataEvent> tapdataEvents) {
		dispatchTapdataEvents(
				tapdataEvents,
				consumeEvents -> {
					if (!inCdc) {
						List<TapdataEvent> partialCdcEvents = new ArrayList<>();
						final Iterator<TapdataEvent> iterator = consumeEvents.iterator();
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
							initialProcessEvents(consumeEvents, false);
							// process partial cdc event
							if (this.initialPartitionConcurrentProcessor != null) {
								this.initialPartitionConcurrentProcessor.stop();
							}
							cdcProcessEvents(partialCdcEvents);
						} else {
							initialProcessEvents(consumeEvents, true);
						}
					} else {
						cdcProcessEvents(consumeEvents);
					}
				}
		);
	}

	private void queueConsume() {
		try {
			Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			long lastProcessTime = System.currentTimeMillis();
			while (isRunning()) {
				TapdataEvent tapdataEvent = tapEventQueue.poll(1L, TimeUnit.SECONDS);
				if (null != tapdataEvent) {
					tapdataEvents.add(tapdataEvent);
				}
				if (tapdataEvents.size() >= this.targetBatch) {
					processTargetEvents(tapdataEvents);
					tapdataEvents.clear();
					lastProcessTime = System.currentTimeMillis();
				}
				if (System.currentTimeMillis() - lastProcessTime >= targetBatchIntervalMs && CollectionUtils.isNotEmpty(tapdataEvents)) {
					processTargetEvents(tapdataEvents);
					tapdataEvents.clear();
					lastProcessTime = System.currentTimeMillis();
				}
			}
		} catch (InterruptedException ignored) {
		} catch (Throwable e) {
			errorHandle(e, "Target process failed " + e.getMessage());
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
		AtomicReference<TapdataEvent> lastDmlTapdataEvent = new AtomicReference<>();
		try {
			List<TapEvent> tapEvents = new ArrayList<>();
			List<TapdataShareLogEvent> tapdataShareLogEvents = new ArrayList<>();
			if (null != getConnectorNode()) {
				codecsFilterManager = getConnectorNode().getCodecsFilterManager();
			}
			lastDmlTapdataEvent = new AtomicReference<>();
			for (TapdataEvent tapdataEvent : tapdataEvents) {
				if (!isRunning()) return;
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
					} else if (tapdataEvent instanceof TapdataTaskErrorEvent) {
						throw ((TapdataTaskErrorEvent) tapdataEvent).getThrowable();
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
					throw new RuntimeException(String.format("Handle events failed: %s", throwable.getMessage()), throwable);
				}
			}
			if (CollectionUtils.isNotEmpty(tapEvents)) {
				try {
					processEvents(tapEvents);
				} catch (Throwable throwable) {
					throw new RuntimeException(String.format("Process events failed: %s", throwable.getMessage()), throwable);
				}
			}
			if (CollectionUtils.isNotEmpty(tapdataShareLogEvents)) {
				try {
					processShareLog(tapdataShareLogEvents);
				} catch (Throwable throwable) {
					throw new RuntimeException(String.format("Process share log failed: %s", throwable.getMessage()), throwable);
				}
			}
		} finally {
			flushSyncProgressMap(lastDmlTapdataEvent.get());
		}
	}

	private void handleTapdataShareLogEvent(List<TapdataShareLogEvent> tapdataShareLogEvents, TapdataEvent tapdataEvent, Consumer<TapdataEvent> consumer) {
		TapRecordEvent tapRecordEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
		fromTapValue(TapEventUtil.getBefore(tapRecordEvent), codecsFilterManager);
		fromTapValue(TapEventUtil.getAfter(tapRecordEvent), codecsFilterManager);
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
		if (TaskDto.TYPE_INITIAL_SYNC.equals(dataProcessorContext.getTaskDto().getType())) {
			putInGlobalMap(getCompletedInitialKey(), true);
		}
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

		if(!handleUpdateConditionFields(tapRecordEvent)){
			return ;
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

	/**
	 * 处理删除事件更新条件在事件中不存在对应的值
	 */
	private boolean handleUpdateConditionFields(TapRecordEvent tapRecordEvent) {
		if (tapRecordEvent instanceof TapDeleteRecordEvent) {
			TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) tapRecordEvent;
			TapTable tapTable = this.dataProcessorContext.getTapTableMap().get(getTgtTableNameFromTapEvent(tapRecordEvent));
			Collection<String> updateConditionFields = tapTable.primaryKeys(true);
			Map<String, Object> objectMap = tapDeleteRecordEvent.getBefore();
			for (String field : updateConditionFields) {
				// updateConditionField  may appear  x.x.x
				if (field.contains(".")) {
					String[] updateFiled = field.split("\\.");
					if (!objectMap.containsKey(updateFiled[0]) || !(objectMap.get(updateFiled[0]) instanceof Map ||
							objectMap.get(updateFiled[0]) instanceof TapMapValue)) {
						obsLogger.warn("Find table:" + tableName + " delete event,Because there is no association field is : " + field + " ,The delete event will not be updated to the target");
						return false;
					}
					TapMapValue tapMapValue = (TapMapValue) objectMap.get(updateFiled[0]);
					for (int index = 1; index < updateFiled.length; index++) {
						if (index != updateFiled.length - 1 && !(tapMapValue.getValue() instanceof Map ||
								objectMap.get(updateFiled[0]) instanceof TapMapValue)) {
							obsLogger.warn("Find table:" + tableName + " delete event,Because there is no association field is : " + field + " ,The delete event will not be updated to the target");
							return false;
						}
						if (!tapMapValue.getValue().containsKey(updateFiled[index])) {
							obsLogger.warn("Find table:" + tableName + " delete event,Because there is no association field is : " + field + " ,The delete event will not be updated to the target");
							return false;
						} else {
							if (index == updateFiled.length - 1) {
								return true;
							} else
								try {
									tapMapValue = (TapMapValue) tapMapValue.getValue().get(updateFiled[index]);
								} catch (Exception e) {
									obsLogger.warn("Find table:" + tableName + " delete event,Because there is no association field is : " + field + " ,The delete event will not be updated to the target");
									return false;
								}
						}
					}
				} else {
					if (!objectMap.containsKey(field)) {
						obsLogger.warn("Find table:" + tableName + " delete event,Because there is no association field is : " + field + " ,The delete event will not be updated to the target");
						return false;
					}
				}
			}
		}
		return true;
	}

	@NotNull
	private PartitionConcurrentProcessor initConcurrentProcessor(int cdcConcurrentWriteNum) {
		int batchSize = Math.max(this.targetBatch / cdcConcurrentWriteNum, DEFAULT_TARGET_BATCH) * 2;
		return new PartitionConcurrentProcessor(
				cdcConcurrentWriteNum,
				batchSize,
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
			CommonUtils.ignoreAnyError(() -> removeGlobalMap(getCompletedInitialKey()), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.queueConsumerThreadPool).ifPresent(ExecutorService::shutdownNow), TAG);
		} finally {
			super.doClose();
		}
	}
}

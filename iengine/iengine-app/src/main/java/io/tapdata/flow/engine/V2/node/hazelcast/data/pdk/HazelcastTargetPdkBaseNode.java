package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Queues;
import com.hazelcast.jet.core.Inbox;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.StringUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataCompleteSnapshotEvent;
import com.tapdata.entity.TapdataCompleteTableSnapshotEvent;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.TapdataStartedCdcEvent;
import com.tapdata.entity.TapdataStartingCdcEvent;
import com.tapdata.entity.TapdataTaskErrorEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageResult;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import io.tapdata.aspect.CreateTableFuncAspect;
import io.tapdata.aspect.NewFieldFuncAspect;
import io.tapdata.aspect.TaskMilestoneFuncAspect;
import io.tapdata.aspect.supervisor.DataNodeThreadGroupAspect;
import io.tapdata.aspect.taskmilestones.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TapdataEventException;
import io.tapdata.error.TaskTargetProcessorExCode_15;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.exactlyonce.ExactlyOnceUtil;
import io.tapdata.flow.engine.V2.exactlyonce.write.CheckExactlyOnceWriteEnableResult;
import io.tapdata.flow.engine.V2.exactlyonce.write.ExactlyOnceWriteCleaner;
import io.tapdata.flow.engine.V2.exactlyonce.write.ExactlyOnceWriteCleanerEntity;
import io.tapdata.flow.engine.V2.exception.TapExactlyOnceWriteExCode_22;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderController;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderService;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.PartitionConcurrentProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.KeysPartitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.TapEventPartitionKeySelector;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.flow.engine.V2.util.TargetTapEventFilter;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.tapdata.entity.simplify.TapSimplify.createIndexEvent;
import static io.tapdata.entity.simplify.TapSimplify.createTableEvent;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:58
 **/
public abstract class HazelcastTargetPdkBaseNode extends HazelcastPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	public static final long DEFAULT_TARGET_BATCH_INTERVAL_MS = 1000;
	public static final int DEFAULT_TARGET_BATCH = 1000;
	protected Map<String, SyncProgress> syncProgressMap = new ConcurrentHashMap<>();
	private AtomicBoolean firstBatchEvent = new AtomicBoolean();
	private AtomicBoolean firstStreamEvent = new AtomicBoolean();
	protected Map<String, List<String>> updateConditionFieldsMap = new HashMap<>();
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
	private final Object saveSnapshotLock = new Object();
	private final ThreadPoolExecutorEx queueConsumerThreadPool;
	private boolean inCdc = false;
	protected int targetBatch;
	protected long targetBatchIntervalMs;
	private TargetTapEventFilter targetTapEventFilter;
	protected final List<String> exactlyOnceWriteTables = new ArrayList<>();
	protected final ConcurrentHashMap<String, List<String>> exactlyOnceWriteNeedLookupTables = new ConcurrentHashMap<>();
	protected CheckExactlyOnceWriteEnableResult checkExactlyOnceWriteEnableResult;

	private final List<ExactlyOnceWriteCleanerEntity> exactlyOnceWriteCleanerEntities = new ArrayList<>();

	public HazelcastTargetPdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
//        queueConsumerThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), r -> {
//            Thread thread = new Thread(r);
//            thread.setName(String.format("Target-Queue-Consumer-%s[%s]", getNode().getName(), getNode().getId()));
//            return thread;
//        });
		queueConsumerThreadPool = AsyncUtils.createThreadPoolExecutor(String.format("Target-Queue-Consumer-%s[%s]@task-%s", getNode().getName(), getNode().getId(), dataProcessorContext.getTaskDto().getName()), 1, new ConnectorOnTaskThreadGroup(dataProcessorContext), TAG);
		//threadPoolExecutorEx = AsyncUtils.createThreadPoolExecutor("Target-" + getNode().getName() + "@task-" + dataProcessorContext.getTaskDto().getName(), 1, new ConnectorOnTaskThreadGroup(dataProcessorContext), TAG);
		TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.RUNNING);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		initExactlyOnceWriteIfNeed();
		queueConsumerThreadPool.submitSync(() -> {
			super.doInit(context);
			createPdkAndInit(context);
			initTargetVariable();
			initTargetQueueConsumer();
			initTargetConcurrentProcessorIfNeed();
			initTapEventFilter();
		});
	}

	private void initExactlyOnceWriteIfNeed() {
		checkExactlyOnceWriteEnableResult = enableExactlyOnceWrite();
		if (!checkExactlyOnceWriteEnableResult.getEnable()) {
			if (StringUtils.isNotBlank(checkExactlyOnceWriteEnableResult.getMessage())) {
				obsLogger.info("Node({}) exactly once write is disabled, reason: {}", getNode().getName(), checkExactlyOnceWriteEnableResult.getMessage());
			}
			return;
		}
		ConnectorNode connectorNode = getConnectorNode();
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();

		TapTable exactlyOnceTable = ExactlyOnceUtil.generateExactlyOnceTable(getConnectorNode());
		if (null != dataProcessorContext.getTapTableMap()) {
			dataProcessorContext.getTapTableMap().putNew(exactlyOnceTable.getId(), exactlyOnceTable, exactlyOnceTable.getId());
		}
		boolean create = createTable(exactlyOnceTable);
		if (create) {
			obsLogger.info("Create exactly once write cache table: {}", exactlyOnceTable);
			CreateIndexFunction createIndexFunction = connectorFunctions.getCreateIndexFunction();
			TapCreateIndexEvent indexEvent = createIndexEvent(exactlyOnceTable.getId(), exactlyOnceTable.getIndexList());
			PDKInvocationMonitor.invoke(
							getConnectorNode(), PDKMethod.TARGET_CREATE_INDEX,
							() -> createIndexFunction.createIndex(getConnectorNode().getConnectorContext(), exactlyOnceTable, indexEvent), TAG);
		}
		Node node = getNode();
		if (node instanceof TableNode) {
			TableNode tableNode = (TableNode) node;
			String tableName = tableNode.getTableName();
			exactlyOnceWriteTables.add(tableName);
			ExactlyOnceWriteCleanerEntity exactlyOnceWriteCleanerEntity = new ExactlyOnceWriteCleanerEntity(
							tableNode.getId(),
							tableName,
							tableNode.getIncrementExactlyOnceEnableTimeWindowDay(),
							tableNode.getConnectionId()
			);
			ExactlyOnceWriteCleaner.getInstance().registerCleaner(exactlyOnceWriteCleanerEntity);
			exactlyOnceWriteCleanerEntities.add(exactlyOnceWriteCleanerEntity);
			obsLogger.info("Registered exactly once write cleaner: {}", exactlyOnceWriteCleanerEntity);
		} else if (node instanceof DatabaseNode) {
			// Nonsupport
		}
		obsLogger.info("Exactly once write has been enabled, and the effective table is: {}", StringUtil.subLongString(Arrays.toString(exactlyOnceWriteTables.toArray()), 100, "..."));
	}

	protected boolean createTable(TapTable tapTable) {
		AtomicReference<TapCreateTableEvent> tapCreateTableEvent = new AtomicReference<>();
		boolean createdTable;
		try {
			CreateTableFunction createTableFunction = getConnectorNode().getConnectorFunctions().getCreateTableFunction();
			CreateTableV2Function createTableV2Function = getConnectorNode().getConnectorFunctions().getCreateTableV2Function();
			createdTable = createTableV2Function != null || createTableFunction != null;
			if (createdTable) {
				handleTapTablePrimaryKeys(tapTable);
				tapCreateTableEvent.set(createTableEvent(tapTable));
				executeDataFuncAspect(CreateTableFuncAspect.class, () -> new CreateTableFuncAspect()
								.createTableEvent(tapCreateTableEvent.get())
								.connectorContext(getConnectorNode().getConnectorContext())
								.dataProcessorContext(dataProcessorContext)
								.start(), (createTableFuncAspect ->
								PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TARGET_CREATE_TABLE, () -> {
									if (createTableV2Function != null) {
										CreateTableOptions createTableOptions = createTableV2Function.createTable(getConnectorNode().getConnectorContext(), tapCreateTableEvent.get());
										if (createTableFuncAspect != null)
											createTableFuncAspect.createTableOptions(createTableOptions);
									} else {
										createTableFunction.createTable(getConnectorNode().getConnectorContext(), tapCreateTableEvent.get());
									}
								}, TAG)));
			} else {
				// only execute start function aspect so that it would be cheated as input
				AspectUtils.executeAspect(new CreateTableFuncAspect()
								.createTableEvent(tapCreateTableEvent.get())
								.connectorContext(getConnectorNode().getConnectorContext())
								.dataProcessorContext(dataProcessorContext).state(NewFieldFuncAspect.STATE_START));
			}
			//
//			String s = JSONUtil.obj2Json(Collections.singletonList(tapTable));
			clientMongoOperator.insertOne(Collections.singletonList(tapTable),
							ConnectorConstant.CONNECTION_COLLECTION + "/load/part/tables/" + dataProcessorContext.getTargetConn().getId());
		} catch (Throwable throwable) {
			throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_TABLE_FAILED, "Table model: " + tapTable, throwable)
							.addEvent(tapCreateTableEvent.get());
		}
		return createdTable;
	}

	private void initTargetQueueConsumer() {
		this.queueConsumerThreadPool.submit(this::queueConsume);
		obsLogger.debug("Initialize target event handler complete");
	}

	private void initTargetConcurrentProcessorIfNeed() {
		if (getNode() instanceof DataParentNode) {
			DataParentNode dataParentNode = (DataParentNode) getNode();
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

	private void initTargetVariable() {
		this.uploadDagService = new AtomicBoolean(false);
		this.insertMetadata = new CopyOnWriteArrayList<>();
		this.updateMetadata = new ConcurrentHashMap<>();
		this.removeMetadata = new CopyOnWriteArrayList<>();

		targetBatch = DEFAULT_TARGET_BATCH;
		if (getNode() instanceof DataParentNode) {
			this.targetBatch = Optional.ofNullable(((DataParentNode<?>) getNode()).getWriteBatchSize()).orElse(DEFAULT_TARGET_BATCH);
		}

		targetBatchIntervalMs = DEFAULT_TARGET_BATCH_INTERVAL_MS;
		if (getNode() instanceof DataParentNode) {
			this.targetBatchIntervalMs = Optional.ofNullable(((DataParentNode<?>) getNode()).getWriteBatchWaitMs()).orElse(DEFAULT_TARGET_BATCH_INTERVAL_MS);
		}
		obsLogger.info("Write batch size: {}, max wait ms per batch: {}", targetBatch, targetBatchIntervalMs);
		int writeQueueCapacity = new BigDecimal(targetBatch).multiply(new BigDecimal("1.5")).setScale(0, RoundingMode.HALF_UP).intValue();
		this.tapEventQueue = new LinkedBlockingQueue<>(writeQueueCapacity);
		obsLogger.debug("Initialize target write queue complete, capacity: {}", writeQueueCapacity);
	}

	private void createPdkAndInit(@NotNull Context context) {
		if (getNode() instanceof TableNode || getNode() instanceof DatabaseNode) {
			try {
				createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
				connectorNodeInit(dataProcessorContext);
			} catch (Throwable e) {
				throw new NodeException(e).context(getProcessorBaseContext());
			}
		}
	}

	private void initTapEventFilter() {
		this.targetTapEventFilter = TargetTapEventFilter.create();
		//this.targetTapEventFilter.addFilter(new DeleteConditionFieldFilter());
	}

	@Override
	final public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			Thread.currentThread().setName(String.format("Target-Process-%s[%s]", getNode().getName(), getNode().getId()));
			if (!inbox.isEmpty()) {
				List<TapdataEvent> tapdataEvents = new ArrayList<>();
				final int count = inbox.drainTo(tapdataEvents, targetBatch);
				if (count > 0) {
					for (TapdataEvent tapdataEvent : tapdataEvents) {
						// Filter TapEvent
						if (null != tapdataEvent.getTapEvent() && this.targetTapEventFilter.test(tapdataEvent)) {
							if (tapdataEvent.getSyncStage().equals(SyncStage.CDC)) {
								tapdataEvent = TapdataHeartbeatEvent.create(TapEventUtil.getTimestamp(tapdataEvent.getTapEvent()), tapdataEvent.getStreamOffset(), tapdataEvent.getNodeIds());
							} else {
								continue;
							}
						}
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
			RuntimeException runtimeException = new RuntimeException(String.format("Drain from inbox failed: %s", e.getMessage()), e);
			errorHandle(runtimeException);
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
							if (tapdataEvent instanceof TapdataStartingCdcEvent || inCdc) {
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
			AspectUtils.executeAspect(DataNodeThreadGroupAspect.class, () ->
					new DataNodeThreadGroupAspect(this.getNode(), associateId, Thread.currentThread().getThreadGroup())
							.dataProcessorContext(dataProcessorContext));
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			while (isRunning()) {
				int drain = Queues.drain(tapEventQueue, tapdataEvents, targetBatch, targetBatchIntervalMs, TimeUnit.MILLISECONDS);
				if (drain > 0) {
					processTargetEvents(tapdataEvents);
					tapdataEvents.clear();
				}
			}
		} catch (InterruptedException ignored) {
		} catch (Throwable e) {
			executeAspect(WriteErrorAspect.class, () -> new WriteErrorAspect().dataProcessorContext(dataProcessorContext).error(e));
			Throwable throwableWrapper;
			if (!(e instanceof TapCodeException)) {
				throwableWrapper = new TapCodeException(TaskTargetProcessorExCode_15.UNKNOWN_ERROR, e);
			} else {
				throwableWrapper = e;
			}
			errorHandle(throwableWrapper, null);
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
		AtomicReference<TapdataEvent> lastTapdataEvent = new AtomicReference<>();
		List<TapEvent> tapEvents = new ArrayList<>();
		List<TapRecordEvent> exactlyOnceWriteCache = new ArrayList<>();
		List<TapdataShareLogEvent> tapdataShareLogEvents = new ArrayList<>();
		if (null != getConnectorNode()) {
			codecsFilterManager = getConnectorNode().getCodecsFilterManager();
		}
		initAndGetExactlyOnceWriteLookupList();
		boolean hasExactlyOnceWriteCache = false;
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			if (!isRunning()) return;
			try {
				SyncStage syncStage = tapdataEvent.getSyncStage();
				if (null != syncStage) {
					if (syncStage == SyncStage.INITIAL_SYNC && firstBatchEvent.compareAndSet(false, true)) {
						executeAspect(new SnapshotWriteBeginAspect().dataProcessorContext(dataProcessorContext));
					} else if (syncStage == SyncStage.CDC && firstStreamEvent.compareAndSet(false, true)) {
						executeAspect(new CDCWriteBeginAspect().dataProcessorContext(dataProcessorContext));
					}
				}
				if (tapdataEvent instanceof TapdataHeartbeatEvent) {
					handleTapdataHeartbeatEvent(tapdataEvent);
				} else if (tapdataEvent instanceof TapdataCompleteSnapshotEvent) {
					handleTapdataCompleteSnapshotEvent();
				} else if (tapdataEvent instanceof TapdataStartingCdcEvent) {
					handleTapdataStartCdcEvent(tapdataEvent);
				} else if (tapdataEvent instanceof TapdataStartedCdcEvent) {
					flushShareCdcTableMetrics(tapdataEvent);
				} else if (tapdataEvent instanceof TapdataTaskErrorEvent) {
					throw ((TapdataTaskErrorEvent) tapdataEvent).getThrowable();
				} else if (tapdataEvent instanceof TapdataShareLogEvent) {
					handleTapdataShareLogEvent(tapdataShareLogEvents, tapdataEvent, lastTapdataEvent::set);
				} else if (tapdataEvent instanceof TapdataCompleteTableSnapshotEvent) {
					handleTapdataCompleteTableSnapshotEvent((TapdataCompleteTableSnapshotEvent) tapdataEvent);
				} else {
					if (tapdataEvent.isDML()) {
						TapRecordEvent tapRecordEvent = handleTapdataRecordEvent(tapdataEvent);
						if (null == tapRecordEvent) {
							continue;
						}
						hasExactlyOnceWriteCache = handleExactlyOnceWriteCacheIfNeed(tapdataEvent, exactlyOnceWriteCache);
						List<String> lookupTables = initAndGetExactlyOnceWriteLookupList();
						String tgtTableNameFromTapEvent = getTgtTableNameFromTapEvent(tapRecordEvent);
						if (null != lookupTables && lookupTables.contains(tgtTableNameFromTapEvent) && hasExactlyOnceWriteCache && eventExactlyOnceWriteCheckExists(tapdataEvent)) {
							if (obsLogger.isDebugEnabled()) {
								obsLogger.debug("Event check exactly once write exists, will ignore it: {}" + JSONUtil.obj2Json(tapRecordEvent));
							}
							continue;
						} else {
							if (SyncStage.CDC.equals(tapdataEvent.getSyncStage()) && null != lookupTables && lookupTables.contains(tgtTableNameFromTapEvent)) {
								obsLogger.info("Target table {} stop look up exactly once cache", tgtTableNameFromTapEvent);
								lookupTables.remove(tgtTableNameFromTapEvent);
							}
						}
						tapEvents.add(tapRecordEvent);
						if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
							lastTapdataEvent.set(tapdataEvent);
						}
					} else if (tapdataEvent.isDDL()) {
						handleTapdataDDLEvent(tapdataEvent, tapEvents, lastTapdataEvent::set);
					} else {
						if (null != tapdataEvent.getTapEvent()) {
							obsLogger.warn("Tap event type does not supported: " + tapdataEvent.getTapEvent().getClass() + ", will ignore it");
						}
					}
				}
			} catch (Throwable throwable) {
				throw new TapdataEventException(TaskTargetProcessorExCode_15.HANDLE_EVENTS_FAILED, throwable).addEvent(tapdataEvent);
			}
		}
		if (CollectionUtils.isNotEmpty(tapEvents)) {
			try {
				try {
					if (checkExactlyOnceWriteEnableResult.getEnable() && hasExactlyOnceWriteCache) {
						transactionBegin();
					}
					processEvents(tapEvents);
					if (checkExactlyOnceWriteEnableResult.getEnable() && hasExactlyOnceWriteCache) {
						processExactlyOnceWriteCache(tapdataEvents);
						transactionCommit();
					}
				} catch (Exception e) {
					transactionRollback();
					throw e;
				}
				flushOffsetByTapdataEventForNoConcurrent(lastTapdataEvent);
			} catch (Throwable throwable) {
				throw new RuntimeException(String.format("Process events failed: %s", throwable.getMessage()), throwable);
			}
		}
		if (CollectionUtils.isNotEmpty(tapdataShareLogEvents)) {
			try {
				processShareLog(tapdataShareLogEvents);
				flushOffsetByTapdataEventForNoConcurrent(lastTapdataEvent);
			} catch (Throwable throwable) {
				throw new RuntimeException(String.format("Process share log failed: %s", throwable.getMessage()), throwable);
			}
		}
		if (firstStreamEvent.get()) {
			executeAspect(new CDCHeartbeatWriteAspect().tapdataEvents(tapdataEvents).dataProcessorContext(dataProcessorContext));
		}
	}

	private void handleTapdataCompleteTableSnapshotEvent(TapdataCompleteTableSnapshotEvent tapdataEvent) {
		List<String> nodeIds = tapdataEvent.getNodeIds();
		if (CollectionUtils.isEmpty(nodeIds) && nodeIds.size() >= 1) {
			return;
		}
		String srcNodeId = nodeIds.get(0);
		Node srcNode = dataProcessorContext.getNodes().stream().filter(n -> n.getId().equals(srcNodeId)).findFirst().orElse(null);
		if (null == srcNode) {
			return;
		}
		SnapshotOrderController snapshotOrderController = SnapshotOrderService.getInstance().getController(dataProcessorContext.getTaskDto().getId().toHexString());
		if (null != snapshotOrderController) {
			snapshotOrderController.finish(srcNode);
			snapshotOrderController.flush();
		}
		executeAspect(new SnapshotWriteTableCompleteAspect().sourceNodeId(srcNodeId).sourceTableName(tapdataEvent.getSourceTableName()).dataProcessorContext(dataProcessorContext));
	}

	private void flushOffsetByTapdataEventForNoConcurrent(AtomicReference<TapdataEvent> lastTapdataEvent) {
		if (null != lastTapdataEvent.get()) {
			SyncStage syncStage = lastTapdataEvent.get().getSyncStage();
			if (null != syncStage) {
				switch (syncStage) {
					case INITIAL_SYNC:
						if (null != lastTapdataEvent.get().getBatchOffset() && !initialConcurrent) {
							flushSyncProgressMap(lastTapdataEvent.get());
						}
						break;
					case CDC:
						if (null != lastTapdataEvent.get().getStreamOffset() && !cdcConcurrent) {
							flushSyncProgressMap(lastTapdataEvent.get());
						}
						break;
				}
			}
		}
	}

	private void flushShareCdcTableMetrics(TapdataEvent tapdataEvent) {
		if (tapdataEvent.getType().equals(SyncProgress.Type.LOG_COLLECTOR)) {
			Object connIdObj = tapdataEvent.getInfo(TapdataStartedCdcEvent.CONNECTION_ID_INFO_KEY);
			String connectionId = "";
			if (connIdObj instanceof String) {
				connectionId = (String) connIdObj;
			}
			Object tableNamesObj = tapdataEvent.getInfo(TapdataEvent.TABLE_NAMES_INFO_KEY);
			List<ShareCdcTableMetricsDto> shareCdcTableMetricsDtoList = new ArrayList<>();
			if (tableNamesObj instanceof List
					&& CollectionUtils.isNotEmpty((List<?>) tableNamesObj)) {
				for (Object tableNameObj : (List<?>) tableNamesObj) {
					if (!(tableNameObj instanceof String)) {
						continue;
					}
					ShareCdcTableMetricsDto shareCdcTableMetricsDto = new ShareCdcTableMetricsDto();
					shareCdcTableMetricsDto.setTaskId(dataProcessorContext.getTaskDto().getId().toHexString());
					shareCdcTableMetricsDto.setNodeId(tapdataEvent.getNodeIds().get(0));
					shareCdcTableMetricsDto.setConnectionId(connectionId);
					shareCdcTableMetricsDto.setTableName((String) tableNameObj);
					shareCdcTableMetricsDto.setStartCdcTime(((TapdataStartedCdcEvent) tapdataEvent).getCdcStartTime());

					shareCdcTableMetricsDtoList.add(shareCdcTableMetricsDto);
					if (shareCdcTableMetricsDtoList.size() == 10) {
						clientMongoOperator.insertMany(shareCdcTableMetricsDtoList, ConnectorConstant.SHARE_CDC_TABLE_METRICS_COLLECTION + "/saveOrUpdateDaily");
						shareCdcTableMetricsDtoList.clear();
					}
				}
				if (CollectionUtils.isNotEmpty(shareCdcTableMetricsDtoList)) {
					clientMongoOperator.insertMany(shareCdcTableMetricsDtoList, ConnectorConstant.SHARE_CDC_TABLE_METRICS_COLLECTION + "/saveOrUpdateDaily");
					shareCdcTableMetricsDtoList.clear();
				}
			}
		}
	}

	private void handleTapdataShareLogEvent(List<TapdataShareLogEvent> tapdataShareLogEvents, TapdataEvent tapdataEvent, Consumer<TapdataEvent> consumer) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		if (tapEvent instanceof TapRecordEvent) {
			TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
			fromTapValue(TapEventUtil.getBefore(tapRecordEvent), codecsFilterManager);
			fromTapValue(TapEventUtil.getAfter(tapRecordEvent), codecsFilterManager);
		}
		tapdataShareLogEvents.add((TapdataShareLogEvent) tapdataEvent);
		if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
			consumer.accept(tapdataEvent);
		}
	}

	private void handleTapdataStartCdcEvent(TapdataEvent tapdataEvent) {
		flushSyncProgressMap(tapdataEvent);
		saveToSnapshot();
	}

	protected void handleTapdataCompleteSnapshotEvent() {
		Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE.getTaskGlobalVariable(dataProcessorContext.getTaskDto().getId().toHexString());
		Object obj = taskGlobalVariable.get(TaskGlobalVariable.SOURCE_INITIAL_COUNTER_KEY);
		if (obj instanceof AtomicInteger) {
			((AtomicInteger) obj).decrementAndGet();
		}
		executeAspect(new SnapshotWriteEndAspect().dataProcessorContext(dataProcessorContext));
	}

	private void handleTapdataHeartbeatEvent(TapdataEvent tapdataEvent) {
		flushSyncProgressMap(tapdataEvent);
		saveToSnapshot();
	}

	private TapRecordEvent handleTapdataRecordEvent(TapdataEvent tapdataEvent) {
		TapRecordEvent tapRecordEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
		if (writeStrategy.equals(MergeTableProperties.MergeType.appendWrite.name())) {
			// When append write, update and delete event turn into insert event
			if (tapRecordEvent instanceof TapUpdateRecordEvent) {
				Map<String, Object> after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
				TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create();
				tapRecordEvent.clone(tapInsertRecordEvent);
				tapInsertRecordEvent.setAfter(after);
				tapRecordEvent = tapInsertRecordEvent;
			} else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
				Map<String, Object> before = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
				TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create();
				tapRecordEvent.clone(tapInsertRecordEvent);
				tapInsertRecordEvent.setAfter(before);
				tapRecordEvent = tapInsertRecordEvent;
			}
		}
		fromTapValue(TapEventUtil.getBefore(tapRecordEvent), codecsFilterManager);
		fromTapValue(TapEventUtil.getAfter(tapRecordEvent), codecsFilterManager);
		return tapRecordEvent;
	}

	private boolean handleExactlyOnceWriteCacheIfNeed(TapdataEvent tapdataEvent, List<TapRecordEvent> exactlyOnceWriteCache) {
		if (!tableEnableExactlyOnceWrite(tapdataEvent.getSyncStage(), getTgtTableNameFromTapEvent(tapdataEvent.getTapEvent()))) {
			return false;
		}
		if (null == exactlyOnceWriteCache) {
			throw new IllegalArgumentException("Exactly once write cache list is null");
		}
		TapRecordEvent tapEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
		Long timestamp = TapEventUtil.getTimestamp(tapEvent);
		if (null == timestamp) {
			throw new TapCodeException(TapExactlyOnceWriteExCode_22.WRITE_CACHE_FAILED_TIMESTAMP_IS_NULL, "Event: " + tapEvent);
		}
		Map<String, Object> data = ExactlyOnceUtil.generateExactlyOnceCacheRow(getNode().getId(), getTgtTableNameFromTapEvent(tapdataEvent.getTapEvent()), tapEvent, timestamp);
		TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create()
				.after(data)
				.referenceTime(System.currentTimeMillis())
				.init();
		tapdataEvent.setExactlyOnceWriteCache(tapInsertRecordEvent);
		return true;
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
		if (tapdataEvent instanceof TapdataStartingCdcEvent) {
			if (null == tapdataEvent.getSyncStage()) return;
			syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
		} else if (tapdataEvent instanceof TapdataHeartbeatEvent) {
			if (null == tapdataEvent.getSyncStage()) return;
			syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
			if (null != tapdataEvent.getStreamOffset()) {
				syncProgress.setStreamOffsetObj(tapdataEvent.getStreamOffset());
			}
			syncProgress.setSourceTime(tapdataEvent.getSourceTime());
			syncProgress.setEventTime(tapdataEvent.getSourceTime());
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
			syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
			syncProgress.setSourceTime(tapdataEvent.getSourceTime());
			if (tapdataEvent.getTapEvent() instanceof TapRecordEvent) {
				syncProgress.setEventTime(((TapRecordEvent) tapdataEvent.getTapEvent()).getReferenceTime());
			}
			syncProgress.setType(tapdataEvent.getType());
			flushOffset.set(true);
		}
		syncProgress.setEventSerialNo(syncProgress.addAndGetSerialNo(1));
		if (syncProgress.getSyncStage() == null) {
			obsLogger.warn(String.format("Found sync stage is null when flush sync progress, event: %s[%s]", tapdataEvent, tapdataEvent.getClass().getName()));
		}
	}

	protected void handleTapTablePrimaryKeys(TapTable tapTable) {
		if (writeStrategy.equals(com.tapdata.tm.commons.task.dto.MergeTableProperties.MergeType.updateOrInsert.name())) {
			List<String> updateConditionFields = updateConditionFieldsMap.get(tapTable.getId());
			if (CollectionUtils.isNotEmpty(updateConditionFields)) {
				Collection<String> pks = tapTable.primaryKeys();
				if (!usePkAsUpdateConditions(updateConditionFields, pks)) {
					ignorePksAndIndices(tapTable);
				}
				// 设置逻辑主键
				tapTable.setLogicPrimaries(updateConditionFields);
			} else {
				Collection<String> logicUniqueKey = tapTable.primaryKeys(true);
				if (CollectionUtils.isEmpty(logicUniqueKey)) {
					tapTable.setLogicPrimaries(Collections.emptyList());
				}
			}
		} else if (writeStrategy.equals(com.tapdata.tm.commons.task.dto.MergeTableProperties.MergeType.appendWrite.name())) {
			// 没有关联条件，清空主键信息
			ignorePksAndIndices(tapTable);
		}
	}

	private static void ignorePksAndIndices(TapTable tapTable) {
		tapTable.getNameFieldMap().values().forEach(v -> {
			v.setPrimaryKeyPos(0);
			v.setPrimaryKey(false);
		});
		tapTable.setLogicPrimaries(null);
		tapTable.setIndexList(null);
	}

	@Override
	public boolean saveToSnapshot() {
		try {
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
				obsLogger.warn("Save to snapshot failed, collection: {}, object: {}, errors: {}", collection, this.syncProgressMap, e.getMessage());
				return false;
			}
			if (uploadDagService.get()) {
				synchronized (this.saveSnapshotLock) {
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
			}
		} catch (Throwable throwable) {
			errorHandle(throwable, null);
		} finally {
			ThreadContext.clearAll();
		}
		return true;
	}

	private class DeleteConditionFieldFilter implements TargetTapEventFilter.TapEventPredicate {
		private String tableName;
		private String missingField;
		private Map<String, Object> record;

		/**
		 * 处理删除事件更新条件在事件中不存在对应的值
		 */
		@Override
		public <E extends TapdataEvent> boolean test(E tapdataEvent) {
			if (null == tapdataEvent || null == tapdataEvent.getTapEvent()) return false;
			if (SyncProgress.Type.LOG_COLLECTOR == tapdataEvent.getType()) return false;
			TapEvent tapEvent = tapdataEvent.getTapEvent();
			if (!(tapEvent instanceof TapDeleteRecordEvent)) return false;
			TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) tapEvent;
			this.tableName = getTgtTableNameFromTapEvent(tapEvent);
			TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableName);
			handleTapTablePrimaryKeys(tapTable);
			Collection<String> updateConditionFields = tapTable.primaryKeys(true);
			this.record = tapDeleteRecordEvent.getBefore();
			for (String field : updateConditionFields) {
				// updateConditionField  may appear  x.x.x
				if (field.contains(".")) {
					String[] updateField = field.split("\\.");
					if (!record.containsKey(updateField[0]) || !(record.get(updateField[0]) instanceof Map ||
							record.get(updateField[0]) instanceof TapMapValue)) {
						this.missingField = field;
						return true;
					}
					TapMapValue tapMapValue = (TapMapValue) record.get(updateField[0]);
					for (int index = 1; index < updateField.length; index++) {
						if (index != updateField.length - 1 && !(tapMapValue.getValue() instanceof Map ||
								record.get(updateField[0]) instanceof TapMapValue)) {
							this.missingField = field;
							return true;
						}
						if (!tapMapValue.getValue().containsKey(updateField[index])) {
							this.missingField = field;
							return true;
						} else {
							if (index == updateField.length - 1) {
								return true;
							} else
								try {
									tapMapValue = (TapMapValue) tapMapValue.getValue().get(updateField[index]);
								} catch (Exception e) {
									this.missingField = field;
									return true;
								}
						}
					}
				} else {
					if (!record.containsKey(field)) {
						this.missingField = field;
						return true;
					}
				}
			}
			return false;
		}

		@Override
		public <E extends TapdataEvent> void failHandler(E tapdataEvent) {
			obsLogger.warn("Found {}'s delete event will be ignore. Because there is no association field '{}' in before data: {}", this.tableName, this.missingField, this.record);
		}
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
			if (CollectionUtils.isNotEmpty(exactlyOnceWriteCleanerEntities)) {
				CommonUtils.ignoreAnyError(() -> {
					ExactlyOnceWriteCleaner exactlyOnceWriteCleaner = ExactlyOnceWriteCleaner.getInstance();
					exactlyOnceWriteCleanerEntities.forEach(exactlyOnceWriteCleaner::unregisterCleaner);
				}, TAG);
			}
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.initialPartitionConcurrentProcessor).ifPresent(PartitionConcurrentProcessor::forceStop), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.cdcPartitionConcurrentProcessor).ifPresent(PartitionConcurrentProcessor::forceStop), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.queueConsumerThreadPool).ifPresent(ExecutorService::shutdownNow), TAG);
		} finally {
			super.doClose();
		}
	}

	protected boolean usePkAsUpdateConditions(Collection<String> updateConditions, Collection<String> pks) {
		if (pks == null) {
			pks = Collections.emptySet();
		}
		for (String updateCondition : updateConditions) {
			if (!pks.contains(updateCondition)) {
				return false;
			}
		}
		return true;
	}

	protected CheckExactlyOnceWriteEnableResult enableExactlyOnceWrite() {
		// Check whether the table supports exactly once write
		Node node = getNode();
		if (node instanceof TableNode) {
			TableNode tableNode = (TableNode) getNode();
			if (null == tableNode.getIncrementExactlyOnceEnable() || !tableNode.getIncrementExactlyOnceEnable()) {
				return CheckExactlyOnceWriteEnableResult.createDisable("");
			}
		} else {
			// Other data node type nonsupport exactly once write
			return CheckExactlyOnceWriteEnableResult.createDisable(String.format("Node type %s nonsupport exactly once write", node.getClass().getSimpleName()));
		}

		// Check whether the connector supports exactly once write functions
		ConnectorNode connectorNode = getConnectorNode();
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		TransactionBeginFunction transactionBeginFunction = connectorFunctions.getTransactionBeginFunction();
		TransactionCommitFunction transactionCommitFunction = connectorFunctions.getTransactionCommitFunction();
		TransactionRollbackFunction transactionRollbackFunction = connectorFunctions.getTransactionRollbackFunction();
		QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();
		if (null == transactionBeginFunction || null == transactionCommitFunction || null == transactionRollbackFunction) {
			return CheckExactlyOnceWriteEnableResult.createDisable("The connector nonsupport exactly once write transaction functions: begin, commit, rollback");
		}
		if (null == queryByAdvanceFilterFunction) {
			return CheckExactlyOnceWriteEnableResult.createDisable("The connector is not support exactly once write functions: query by advance filter");
		}

		// Check only have one source node
		List<Node<?>> predecessors = GraphUtil.predecessors(node, Node::isDataNode);
		if (predecessors.size() > 1) {
			return CheckExactlyOnceWriteEnableResult.createDisable("Exactly once write is not supported in any merge scenarios");
		}

		if (CollectionUtils.isNotEmpty(predecessors)) {
			Node<?> sourceNode = predecessors.get(0);
			if (sourceNode instanceof TableNode) {
				String connectionId = ((TableNode) sourceNode).getConnectionId();
				Connections sourceConn = clientMongoOperator.findOne(Query.query(Criteria.where("_id").is(connectionId)), ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
				DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, sourceConn.getPdkHash());
				List<Capability> capabilities = databaseType.getCapabilities();
				if (null == capabilities
						|| null == capabilities.stream().map(Capability::getId).filter(capabilityId -> capabilityId.equals(ConnectionOptions.CAPABILITY_SOURCE_SUPPORT_EXACTLY_ONCE)).findFirst().orElse(null)) {
					return CheckExactlyOnceWriteEnableResult.createDisable(String.format("Source connector(%s) stream read is not supported exactly once", sourceConn.getName()));
				}
			} else if (sourceNode instanceof DatabaseNode) {
				return CheckExactlyOnceWriteEnableResult.createDisable(String.format("Exactly once write is not supported, source connector(%s) is not a table node", sourceNode.getName()));
			}
		}
		return CheckExactlyOnceWriteEnableResult.createEnable();
	}

	private boolean tableEnableExactlyOnceWrite(SyncStage syncStage, String tableId) {
		return SyncStage.CDC.equals(syncStage) && exactlyOnceWriteTables.contains(tableId);
	}

	private List<String> initAndGetExactlyOnceWriteLookupList() {
		return exactlyOnceWriteNeedLookupTables.computeIfAbsent(Thread.currentThread().getName(), k -> {
			Node node = getNode();
			if (node instanceof TableNode) {
				TableNode tableNode = (TableNode) node;
				String tableName = tableNode.getTableName();
				List<String> tables = new ArrayList<>();
				if (exactlyOnceWriteTables.contains(tableName)) {
					tables.add(tableName);
				}
				return tables;
			} else if (node instanceof DatabaseNode) {
				// Nonsupport
			}
			return null;
		});
	}

	abstract void processEvents(List<TapEvent> tapEvents);

	void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents) {
		throw new UnsupportedOperationException();
	}

	void transactionBegin() {
		throw new UnsupportedOperationException();
	}

	void transactionCommit() {
		throw new UnsupportedOperationException();
	}

	void transactionRollback() {
		throw new UnsupportedOperationException();
	}

	void processExactlyOnceWriteCache(List<TapdataEvent> tapdataEvents) {
		throw new UnsupportedOperationException();
	}

	boolean eventExactlyOnceWriteCheckExists(TapdataEvent tapdataEvent) {
		throw new UnsupportedOperationException();
	}

	protected void dispatchTapRecordEvents(List<TapEvent> tapEvents, Predicate<DispatchEntity> dispatchClause, Consumer<List<TapEvent>> consumer) {
		DispatchEntity dispatchEntity = new DispatchEntity();
		List<TapEvent> tempList = new ArrayList<>();
		for (TapEvent tapEvent : tapEvents) {
			if (tapEvent instanceof TapRecordEvent) {
				TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
				if (null == dispatchEntity.getLastTapEvent()) {
					dispatchEntity.setLastTapEvent(tapRecordEvent);
				}
				dispatchEntity.setCurrentTapEvent(tapRecordEvent);
				if (null != dispatchClause && dispatchClause.test(dispatchEntity)) {
					if (CollectionUtils.isNotEmpty(tempList)) {
						consumer.accept(tempList);
						tempList.clear();
					}
					dispatchEntity.setLastTapEvent(tapRecordEvent);
				}
				tempList.add(tapEvent);
			} else if (tapEvent instanceof TapDDLEvent) {
				if (CollectionUtils.isNotEmpty(tempList)) {
					consumer.accept(tempList);
					tempList.clear();
				}
				tempList.add(tapEvent);
				consumer.accept(tempList);
				tempList.clear();
			}
		}
		if (CollectionUtils.isNotEmpty(tempList)) {
			consumer.accept(tempList);
			tempList.clear();
		}
	}

	protected static class DispatchEntity {
		private TapEvent lastTapEvent;
		private TapEvent currentTapEvent;

		public TapEvent getLastTapEvent() {
			return lastTapEvent;
		}

		public void setLastTapEvent(TapEvent lastTapEvent) {
			this.lastTapEvent = lastTapEvent;
		}

		public TapEvent getCurrentTapEvent() {
			return currentTapEvent;
		}

		public void setCurrentTapEvent(TapEvent currentTapEvent) {
			this.currentTapEvent = currentTapEvent;
		}
	}
}

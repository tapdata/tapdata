package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.utils.TimeTransFormationUtil;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.aspect.*;
import io.tapdata.aspect.taskmilestones.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderController;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderService;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryConstant;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryExCode_25;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustResult;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.sharecdc.*;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcReaderExCode_13;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.flow.engine.V2.sharecdc.impl.ShareCdcFactory;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.pdk.core.utils.RetryUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.createIndexEvent;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author jackin
 * @date 2022/2/22 2:33 PM
 **/
public class HazelcastSourcePdkDataNode extends HazelcastSourcePdkBaseNode {
	private static final String TAG = HazelcastSourcePdkDataNode.class.getSimpleName();
	//	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkDataNode.class);
	private final Logger logger = LogManager.getRootLogger();
	private static final int CDC_POLLING_MIN_INTERVAL_MS = 500;
	private static final int CDC_POLLING_MIN_BATCH_SIZE = 1000;
	private static final long BATCH_COUNT_LIMIT = 5000000;
	private static final int EQUAL_VALUE = 5;
	private ShareCdcReader shareCdcReader;
	private final SourceStateAspect sourceStateAspect;
	private List<String> conditionFields;
	private StreamReadConsumer streamReadConsumer;
	private PDKMethodInvoker streamReadMethodInvoker;
	private SyncProgress.Type syncProgressType = SyncProgress.Type.NORMAL;

	public HazelcastSourcePdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		sourceStateAspect = new SourceStateAspect().dataProcessorContext(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		try {
			super.doInit(context);
			checkPollingCDCIfNeed();
		} catch (Throwable e) {
			throw new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, e);
		}
	}

	@Override
	protected void doInitWithDisableNode(@NotNull Context context) throws TapCodeException {
		super.doInitWithDisableNode(context);
		if (getNode().disabledNode() && isRunning()) {
			Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE.getTaskGlobalVariable(dataProcessorContext.getTaskDto().getId().toHexString());
			Object obj = taskGlobalVariable.get(TaskGlobalVariable.SOURCE_INITIAL_COUNTER_KEY);
			if (obj instanceof AtomicInteger) {
				((AtomicInteger) obj).decrementAndGet();
			}
			executeAspect(new SnapshotWriteEndAspect().dataProcessorContext(dataProcessorContext));
			AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED));
		}
	}

	@Override
	public void startSourceRunner() {
		try {
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
			CacheNode cacheNode = (CacheNode) taskDto.getDag().getNodes().stream().filter(node -> node instanceof CacheNode && node.getType().equals(TaskDto.SYNC_TYPE_MEM_CACHE))
					 .findFirst().orElse(null);
			if(cacheNode != null && TaskDto.SYNC_TYPE_MEM_CACHE.equals(taskDto.getSyncType())
					&& cacheNode.getAutoCreateIndex()
					&& CollectionUtils.isNotEmpty(cacheNode.getNeedCreateIndex())){
				for (String tableId : tapTableMap.keySet()) {
					TapTable tapTable = tapTableMap.get(tableId);
					AtomicBoolean succeed = new AtomicBoolean(false);
					if(checkBatchCount(tableId,tapTable)){
						createTargetIndex(cacheNode.getNeedCreateIndex(),succeed.get(),tableId,tapTable);
						Update update = new Update().set("dag.nodes.$.needCreateIndex",new ArrayList<>());
						clientMongoOperator.update(Query.query(Criteria.where("_id").is(taskDto.getId()).and("dag.nodes.id").is(cacheNode.getId())), update, ConnectorConstant.TASK_COLLECTION);
					}else{
						obsLogger.warn("The amount of data is too large and the index cannot be automatically created.");
					}
				}
			}
			try {
				if (need2InitialSync(syncProgress)) {
					if (this.sourceRunnerFirstTime.get()) {
						doSnapshotWithControl(new ArrayList<>(tapTableMap.keySet()));
					}
				}

				if (!sourceRunnerFirstTime.get() && CollectionUtils.isNotEmpty(newTables)) {
					doSnapshot(newTables);
				}

				if (CollectionUtils.isNotEmpty(taskDto.getLdpNewTables())) {
					if (newTables == null) {
						newTables = new CopyOnWriteArrayList<>();
					}
					newTables.addAll(taskDto.getLdpNewTables());
					doSnapshot(newTables);
				}
			} catch (Throwable e) {
				executeAspect(new SnapshotReadErrorAspect().dataProcessorContext(dataProcessorContext).error(e));
				throw e;
			} finally {
				Optional.ofNullable(snapshotProgressManager).ifPresent(SnapshotProgressManager::close);
			}
			Snapshot2CDCAspect.execute(dataProcessorContext);
			if (need2CDC()) {
				waitAllSnapshotCompleteIfNeed();
				try {
					executeAspect(new CDCReadBeginAspect().dataProcessorContext(dataProcessorContext));
					AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_CDC_START));
					doCdc();
					executeAspect(new CDCReadEndAspect().dataProcessorContext(dataProcessorContext));
				} catch (Throwable e) {
					executeAspect(new CDCReadErrorAspect().dataProcessorContext(dataProcessorContext).error(e));
					throw e;
				} finally {
					AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_CDC_COMPLETED));
				}
			} else {
				TapdataTaskScheduler tapdataTaskScheduler = BeanUtil.getBean(TapdataTaskScheduler.class);
				if (null != tapdataTaskScheduler) {
					TaskClient<TaskDto> taskClient = tapdataTaskScheduler.getTaskClient(dataProcessorContext.getTaskDto().getId().toHexString());
					if (null != taskClient) {
						taskClient.terminalMode(TerminalMode.COMPLETE);
					}
				}
			}
		} catch (Throwable throwable) {
			errorHandle(throwable, throwable.getMessage());
		}
	}

	private void waitAllSnapshotCompleteIfNeed() {
		while (isRunning()) {
			if (hasMergeNode() && need2InitialSync(syncProgress)) {
				Predicate<TaskDto> mergeWaitPredicate = taskDto -> {
					Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE.getTaskGlobalVariable(taskDto.getId().toHexString());
					Object obj = taskGlobalVariable.get(TaskGlobalVariable.SOURCE_INITIAL_COUNTER_KEY);
					if (obj instanceof AtomicInteger) {
						return ((AtomicInteger) obj).get() > 0;
					} else {
						return false;
					}
				};
				if (mergeWaitPredicate.test(dataProcessorContext.getTaskDto())) {
					try {
						TimeUnit.SECONDS.sleep(1L);
					} catch (InterruptedException ignored) {
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	}

	private void doSnapshotWithControl(List<String> tableList) throws Throwable {
		SnapshotOrderController controller = SnapshotOrderService.getInstance().getController(dataProcessorContext.getTaskDto().getId().toHexString());
		if (null != controller) {
			CommonUtils.AnyError runner = () -> doSnapshot(tableList);
			controller.runWithControl(getNode(), runner);
		}
	}

	protected void lockBySourceRunnerLock(){
		while (isRunning()) {
			try {
				if (sourceRunnerLock.tryLock(1L, TimeUnit.SECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
	}

	protected void unLockBySourceRunnerLock() {
		try {
			sourceRunnerLock.unlock();
		} catch (Exception e) {
			if (obsLogger.isDebugEnabled()) {
				obsLogger.debug("An error when sourceRunnerLock.unlock(), message: {}", e.getMessage(), e);
			}
		}
	}

	@SneakyThrows
	protected void doSnapshot(List<String> tableList) {
		executeAspect(new SnapshotReadBeginAspect()
				.dataProcessorContext(dataProcessorContext)
				.tables(tableList)
		);
		syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
		ConnectorNode connectorNode = getConnectorNode();
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		BatchCountFunction batchCountFunction = connectorFunctions.getBatchCountFunction();
		DatabaseTypeEnum.DatabaseType databaseType = dataProcessorContext.getDatabaseType();
		if (null == batchCountFunction) {
			setDefaultRowSizeMap();
			obsLogger.warn("PDK node does not support table batch count: {}", databaseType);
		}
		BatchReadFunction batchReadFunction = connectorFunctions.getBatchReadFunction();
		QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();
		ExecuteCommandFunction executeCommandFunction = connectorFunctions.getExecuteCommandFunction();
		if (null == batchReadFunction) {
			throw new NodeException("PDK node does not support batch read: " + databaseType)
					.context(getProcessorBaseContext());
		}

		// MILESTONE-READ_SNAPSHOT-RUNNING
		if (sourceRunnerFirstTime.get()) {
			executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_START));
		}
		try {
			AtomicBoolean firstBatch = new AtomicBoolean(true);
			while (isRunning()) {
				for (String tableName : tableList) {
					TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
					TapTable tapTable = tapTableMap.get(tableName);
					String tableId = tapTable.getId();
					if (BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId)) {
						obsLogger.info("Skip table [{}] in batch read, reason: last task, this table has been completed batch read",
								tableId);
						continue;
					}
					firstBatch.set(true);
					try {
						executeAspect(new SnapshotReadTableBeginAspect().dataProcessorContext(dataProcessorContext).tableName(tableName));
						lockBySourceRunnerLock();
						if (!isRunning()) {
							break;
						}
						if (this.removeTables != null && this.removeTables.contains(tableName)) {
							obsLogger.info("Table {} is detected that it has been removed, the snapshot read will be skipped", tableName);
							this.removeTables.remove(tableName);
							continue;
						}
						obsLogger.info("Starting batch read, table name: {}", tableId);

						PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
						try (AutoCloseable ignoreTableCountCloseable = doAsyncTableCount(batchCountFunction, tableName)) {
							executeDataFuncAspect(
									BatchReadFuncAspect.class, () -> new BatchReadFuncAspect()
											.eventBatchSize(readBatchSize)
											.connectorContext(connectorNode.getConnectorContext())
											.offsetState(null)
											.dataProcessorContext(this.getDataProcessorContext())
											.start()
											.table(tapTable),
									batchReadFuncAspect -> PDKInvocationMonitor.invoke(
											connectorNode,
											PDKMethod.SOURCE_BATCH_READ,
											pdkMethodInvoker.runnable(() -> {
														BiConsumer<List<TapEvent>, Object> consumer = (events, offsetObject) -> {
															if (events != null && !events.isEmpty()) {
																if (firstBatch.compareAndSet(true, false)) {
																	TapdataAdjustMemoryEvent tapdataAdjustMemoryEvent = resizeEventQueueIfNeed(events);
																	if (null != tapdataAdjustMemoryEvent) {
																		enqueue(tapdataAdjustMemoryEvent);
																	}
																}
																events = events.stream().map(event -> {
																	if (null == event.getTime()) {
																		throw new NodeException("Invalid TapEvent, `TapEvent.time` should be NonNUll").context(getProcessorBaseContext()).event(event);
																	}
																	return cdcDelayCalculation.filterAndCalcDelay(event, times -> AspectUtils.executeAspect(SourceCDCDelayAspect.class, () -> new SourceCDCDelayAspect().delay(times).dataProcessorContext(dataProcessorContext)),this.dataProcessorContext.getTaskDto().getSyncType());
																}).collect(Collectors.toList());

																if (batchReadFuncAspect != null)
																	AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_READ_COMPLETE).getReadCompleteConsumers(), events);

																if (obsLogger.isDebugEnabled()) {
																	obsLogger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(connectorNode));
																}
																BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, offsetObject,  TableBatchReadStatus.RUNNING.name());

																flushPollingCDCOffset(events);
																List<TapdataEvent> tapdataEvents = wrapTapdataEvent(events);

																if (batchReadFuncAspect != null)
																	AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_PROCESS_COMPLETE).getProcessCompleteConsumers(), tapdataEvents);

																if (CollectionUtils.isNotEmpty(tapdataEvents)) {
																	tapdataEvents.forEach(this::enqueue);

																	if (batchReadFuncAspect != null)
																		AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_ENQUEUED).getEnqueuedConsumers(), tapdataEvents);
																}
															}
														};
														Node<?> node = getNode();
														if (node instanceof TableNode) {
															TableNode tableNode = (TableNode) dataProcessorContext.getNode();
															if (isTableFilter(tableNode) || isPollingCDC(tableNode)) {
																TapAdvanceFilter tapAdvanceFilter = batchFilterRead();
																queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {
																	List<TapEvent> tempList = new ArrayList<>();
																	if (filterResults != null && CollectionUtils.isNotEmpty(filterResults.getResults())) {
																		filterResults.getResults().forEach(filterResult -> tempList.add(TapSimplify.insertRecordEvent(filterResult, tableId)));
																	}
																	if (CollectionUtils.isNotEmpty(tempList)) {
																		consumer.accept(tempList, null);
																		tempList.clear();
																	}
																});
															} else if (tableNode.isEnableCustomCommand() && executeCommandFunction != null) {
																Map<String, Object> customCommand = tableNode.getCustomCommand();
																customCommand.put("batchSize", readBatchSize);
																executeCommandFunction.execute(connectorNode.getConnectorContext(), TapExecuteCommand.create()
																		.command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params")), executeResult -> {
																	if (executeResult.getError() != null) {
																		throw new NodeException("Execute error: " + executeResult.getError().getMessage(), executeResult.getError());
																	}
																	if (executeResult.getResult() == null) {
																		obsLogger.info("Execute result is null");
																		return;
																	}

																	Object result= executeResult.getResult();
																	handleCustomCommandResult(result,tableName,consumer);
																});
															} else {
																batchReadFunction.batchRead(connectorNode.getConnectorContext(), tapTable, null, readBatchSize, consumer);
															}
														} else {
															batchReadFunction.batchRead(connectorNode.getConnectorContext(), tapTable, null, readBatchSize, consumer);
														}
													}
											)
									));
							BatchOffsetUtil.updateBatchOffset(syncProgress, tableName, null,  TableBatchReadStatus.OVER.name());
							obsLogger.info("Table [{}] has been completed batch read, will skip batch read on the next run", tableName);
						} finally {
							removePdkMethodInvoker(pdkMethodInvoker);
						}
						executeAspect(new SnapshotReadTableEndAspect().dataProcessorContext(dataProcessorContext).tableName(tableName));
						TapdataCompleteTableSnapshotEvent tapdataCompleteTableSnapshotEvent = new TapdataCompleteTableSnapshotEvent(tableName);
						tapdataCompleteTableSnapshotEvent.setBatchOffset(BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableName));
						tapdataCompleteTableSnapshotEvent.setSyncStage(SyncStage.INITIAL_SYNC);
						enqueue(tapdataCompleteTableSnapshotEvent);
					} catch (Throwable throwable) {
						executeAspect(new SnapshotReadTableErrorAspect().dataProcessorContext(dataProcessorContext).tableName(tableName).error(throwable));
						Throwable throwableWrapper = throwable;
						if (!(throwableWrapper instanceof TapCodeException)) {
							throwableWrapper = new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, throwable);
						}
						throw throwableWrapper;
					} finally {
						unLockBySourceRunnerLock();
					}
				}
				try {
					lockBySourceRunnerLock();
					if (CollectionUtils.isNotEmpty(newTables)) {
						tableList.clear();
						tableList.addAll(newTables);
						newTables.clear();
					} else {
						this.endSnapshotLoop.set(true);
						break;
					}
				} finally {
					unLockBySourceRunnerLock();
				}
			}
		} finally {
			if (isRunning()) {
				enqueue(new TapdataCompleteSnapshotEvent());
			}
			AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED));
		}
		executeAspect(new SnapshotReadEndAspect().dataProcessorContext(dataProcessorContext));
	}

	private void handleCustomCommandResult(Object result, String tableName, BiConsumer<List<TapEvent>, Object> consumer){
		if (result instanceof List) {
			List<Map<String, Object>> maps = (List<Map<String, Object>>) result;
			List<TapEvent> events = maps.stream().map(m -> TapSimplify.insertRecordEvent(m, tableName)).collect(Collectors.toList());
			consumer.accept(events, null);
		}else {
			obsLogger.info("The execution result is:{}, because the result is not a list it will be ignored.",result);
		}
	}

	protected void createTargetIndex(List<String> updateConditionFields, boolean createUnique, String tableId, TapTable tapTable) {
		CreateIndexFunction createIndexFunction = getConnectorNode().getConnectorFunctions().getCreateIndexFunction();
		if (null == createIndexFunction) {
			return;
		}
		AtomicReference<TapCreateIndexEvent> indexEvent = new AtomicReference<>();
		try {
			List<TapIndex> tapIndices = new ArrayList<>();
			TapIndex tapIndex = new TapIndex().unique(createUnique);
			List<TapIndexField> tapIndexFields = new ArrayList<>();
			if (null == updateConditionFields) {
				obsLogger.warn("Table " + tableId + " index fields is null, will not create index automatically");
				return;
			}
			if (CollectionUtils.isNotEmpty(updateConditionFields)) {
				boolean usePkAsUpdateConditions = usePkAsUpdateConditions(updateConditionFields, tapTable.primaryKeys());
				if (usePkAsUpdateConditions) {
					return;
				}
				updateConditionFields.forEach(field -> {
					TapIndexField tapIndexField = new TapIndexField();
					tapIndexField.setName(field);
					tapIndexField.setFieldAsc(true);
					tapIndexFields.add(tapIndexField);
				});
				tapIndex.setIndexFields(tapIndexFields);
				tapIndices.add(tapIndex);
				indexEvent.set(createIndexEvent(tableId, tapIndices));

				executeDataFuncAspect(CreateIndexFuncAspect.class, () -> new CreateIndexFuncAspect()
						.table(tapTable)
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.createIndexEvent(indexEvent.get())
						.start(), createIndexFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(),
						PDKMethod.TARGET_CREATE_INDEX,
						() -> createIndexFunction.createIndex(getConnectorNode().getConnectorContext(), tapTable, indexEvent.get()), TAG, buildErrorConsumer(tableId)));
				LoadSchemaRunner loadSchemaRunner = new LoadSchemaRunner(dataProcessorContext.getConnections(), clientMongoOperator, 1);
				loadSchemaRunner.run();
			}
		} catch (Throwable throwable) {
			obsLogger.warn("Automatic index creation failed, please create it manually.");
		}
	}

	private boolean checkBatchCount(String tableId, TapTable tapTable){
		AtomicLong atomicLong = new AtomicLong(0);
		BatchCountFunction batchCountFunction = getConnectorNode().getConnectorFunctions().getBatchCountFunction();
		if (null == batchCountFunction) {
			obsLogger.warn("PDK node does not support table batch count: " + dataProcessorContext.getDatabaseType());
			return false;
		}
		executeDataFuncAspect(TableCountFuncAspect.class, () -> new TableCountFuncAspect()
						.dataProcessorContext(this.getDataProcessorContext())
						.start(),
				tableCountFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_BATCH_COUNT,
						createPdkMethodInvoker().runnable(
								() -> {
									try {
										long count = batchCountFunction.count(getConnectorNode().getConnectorContext(), tapTable);
										atomicLong.set(count);
									} catch (Exception e) {
										throw new NodeException("Count " + tableId + " failed: " + e.getMessage(), e)
												.context(getProcessorBaseContext());
									}
								}
						)
				));
		return atomicLong.get() <= BATCH_COUNT_LIMIT;
	}

	private TapdataAdjustMemoryEvent resizeEventQueueIfNeed(List<TapEvent> events) {
		if (dataProcessorContext.getTaskDto().getDynamicAdjustMemoryUsage()) {
			int newSourceQueueCapacity;
			DynamicAdjustResult dynamicAdjustResult;
			try {
				dynamicAdjustResult = this.dynamicAdjustMemoryService.calcQueueSize(events, this.originalSourceQueueCapacity);
			} catch (Exception e) {
				throw new TapCodeException(DynamicAdjustMemoryExCode_25.UNKNOWN_ERROR, e);
			}
			DynamicAdjustResult.Mode mode = dynamicAdjustResult.getMode();
			if (mode.equals(DynamicAdjustResult.Mode.KEEP)) {
				return null;
			} else if (mode.equals(DynamicAdjustResult.Mode.INCREASE)) {
				newSourceQueueCapacity = originalSourceQueueCapacity;
			} else {
				newSourceQueueCapacity = new BigDecimal(this.originalSourceQueueCapacity)
						.divide(BigDecimal.valueOf(dynamicAdjustResult.getCoefficient()).multiply(new BigDecimal(SOURCE_QUEUE_FACTOR)),
								0, RoundingMode.HALF_UP).intValue();
				newSourceQueueCapacity = Math.max(newSourceQueueCapacity, MIN_QUEUE_SIZE);
			}
			if (newSourceQueueCapacity != this.sourceQueueCapacity) {
				while (isRunning()) {
					if (this.eventQueue.isEmpty()) {
						this.eventQueue = new LinkedBlockingQueue<>(newSourceQueueCapacity);
						obsLogger.info("{}Source queue size adjusted, old size: {}, new size: {}", DynamicAdjustMemoryConstant.LOG_PREFIX, this.sourceQueueCapacity, newSourceQueueCapacity);
						this.sourceQueueCapacity = newSourceQueueCapacity;
						break;
					}
				}
				return new TapdataAdjustMemoryEvent(mode.getValue(), dynamicAdjustResult.getCoefficient());
			}
		}
		return null;
	}

	private static boolean isTableFilter(TableNode tableNode) {
		return tableNode.getIsFilter() && CollectionUtils.isNotEmpty(tableNode.getConditions());
	}

	@SneakyThrows
	protected void doCdc() {
		if (!isRunning()) {
			return;
		}
		AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED));
		enterCDCStage();
		ConnectorNode connectorNode = getConnectorNode();
		if (connectorNode == null) {
			logger.warn("Failed to get source node");
			return;
		}
		streamReadMethodInvoker = createPdkMethodInvoker();
		streamReadConsumer = generateStreamReadConsumer(connectorNode, streamReadMethodInvoker);

		TaskDto taskDto = dataProcessorContext.getTaskDto();
		Node<?> node = dataProcessorContext.getNode();
		if (isPollingCDC(node)) {
			doPollingCDC();
		} else {
			if (node.isLogCollectorNode()) {
				// Mining tasks force traditional increments
				doNormalCDC();
			} else {
				if (taskDto.getShareCdcEnable()) {
					try {
						// Try to start with share cdc
						doShareCdc();
					} catch (ShareCdcUnsupportedException e) {
						if (e.isContinueWithNormalCdc() && !taskDto.getEnforceShareCdc()) {
							// If share cdc is unavailable, and continue with normal cdc is true
							obsLogger.info("Share cdc unusable, will use normal cdc mode, reason: " + e.getMessage());
							try {
								doNormalCDC();
							} catch (Exception ex) {
								throw new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, e);
							}
						} else {
							throw new TapCodeException(ShareCdcReaderExCode_13.UNKNOWN_ERROR, e);
						}
					} catch (Exception e) {
						Throwable throwable = CommonUtils.matchThrowable(e, TapCodeException.class);
						if (null != throwable) {
							throw throwable;
						} else {
							throw new TapCodeException(ShareCdcReaderExCode_13.UNKNOWN_ERROR, e);
						}
					}
				} else {
					doNormalCDC();
				}
			}
		}
	}

	protected void enterCDCStage() {
		this.endSnapshotLoop.set(true);
		if (null == syncProgress.getStreamOffsetObj()) {
			throw new NodeException("Starting stream read failed, errors: start point offset is null").context(getProcessorBaseContext());
		} else {
			TapdataStartingCdcEvent tapdataStartCdcEvent = new TapdataStartingCdcEvent();
			tapdataStartCdcEvent.setSyncStage(SyncStage.CDC);
			tapdataStartCdcEvent.setStreamOffset(syncProgress.getStreamOffsetObj());
			enqueue(tapdataStartCdcEvent);
		}
		// MILESTONE-READ_CDC_EVENT-RUNNING
		TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.RUNNING);
		syncProgress.setSyncStage(SyncStage.CDC.name());
	}

	@SneakyThrows
	protected void doNormalCDC() {
		if (!isRunning()) {
			return;
		}
		syncProgressType = SyncProgress.Type.NORMAL;
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		ConnectorNode connectorNode = getConnectorNode();
		if (connectorNode == null) {
			logger.warn("Failed to get source node");
			return;
		}

		// If 'LogCollectorNode' is merge connection mode then 'connectionConfigWithTables' not null use 'StreamReadMultiConnectionFunction'
		List<ConnectionConfigWithTables> connectionConfigWithTables = ShareCdcUtil.connectionConfigWithTables(getNode(), ids -> {
			Query connectionQuery = new Query(where("_id").in(ids));
			connectionQuery.fields().include("config").include("pdkHash");
			return clientMongoOperator.find(connectionQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
		});
		StreamReadMultiConnectionFunction streamReadMultiConnectionFunction = Optional.ofNullable(connectionConfigWithTables).map(configWithTables -> {
			// first config add heartbeat table to list
			Optional.of(cdcDelayCalculation.addHeartbeatTable(configWithTables.get(0).getTables())).ifPresent(joinHeartbeat -> executeAspect(SourceJoinHeartbeatAspect.class, () -> new SourceJoinHeartbeatAspect().dataProcessorContext(dataProcessorContext).joinHeartbeat(joinHeartbeat)));
			return connectorNode.getConnectorFunctions().getStreamReadMultiConnectionFunction();
		}).orElse(null);

		String streamReadFunctionName = null;
		CommonUtils.AnyError anyError = null;
		List<String> tables = new ArrayList<>();
		if (null != streamReadMultiConnectionFunction) {
			Set<String> tableSet = new HashSet<>();
			for (ConnectionConfigWithTables withTables : connectionConfigWithTables) {
				for (String tableName : withTables.getTables()) {
					tableSet.add(ShareCdcUtil.joinNamespaces(Arrays.asList(
							withTables.getConnectionConfig().getString("schema"), tableName
					)));
				}
			}
			tables.addAll(tableSet);
			excludeRemoveTable(tables);
			streamReadFunctionName = streamReadMultiConnectionFunction.getClass().getSimpleName();
			anyError = () -> {
				streamReadMultiConnectionFunction.streamRead(getConnectorNode().getConnectorContext(), connectionConfigWithTables,
						syncProgress.getStreamOffsetObj(), increaseReadSize, streamReadConsumer);
			};
		} else {
			RawDataCallbackFilterFunction rawDataCallbackFilterFunction = connectorNode.getConnectorFunctions().getRawDataCallbackFilterFunction();
			RawDataCallbackFilterFunctionV2 rawDataCallbackFilterFunctionV2 = connectorNode.getConnectorFunctions().getRawDataCallbackFilterFunctionV2();
//			if(rawDataCallbackFilterFunctionV2 != null) {
//				rawDataCallbackFilterFunction = null;
//			}
			StreamReadFunction streamReadFunction = connectorNode.getConnectorFunctions().getStreamReadFunction();
			if (null != rawDataCallbackFilterFunction || null != rawDataCallbackFilterFunctionV2) {
				if (null != rawDataCallbackFilterFunctionV2) {
					streamReadFunctionName = rawDataCallbackFilterFunctionV2.getClass().getSimpleName();
				} else {
					streamReadFunctionName = rawDataCallbackFilterFunction.getClass().getSimpleName();
				}
				tables.addAll(tapTableMap.keySet());
				excludeRemoveTable(tables);
				Optional.of(cdcDelayCalculation.addHeartbeatTable(tables)).ifPresent(joinHeartbeat -> executeAspect(SourceJoinHeartbeatAspect.class, () -> new SourceJoinHeartbeatAspect().dataProcessorContext(dataProcessorContext).joinHeartbeat(joinHeartbeat)));
				anyError = () -> {
					if (null != streamReadFuncAspect) {
						executeAspect(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_CALLBACK_RAW_DATA).streamReadConsumer(streamReadConsumer));
						while (isRunning()) {
							if (!streamReadFuncAspect.waitRawData()) {
								break;
							}
						}
						if (streamReadFuncAspect.getErrorDuringWait() != null) {
							throw streamReadFuncAspect.getErrorDuringWait();
						}
					}
				};
			} else if (null != streamReadFunction) {
				streamReadFunctionName = streamReadFunction.getClass().getSimpleName();
				tables.addAll(tapTableMap.keySet());
				excludeRemoveTable(tables);
				Optional.of(cdcDelayCalculation.addHeartbeatTable(tables)).ifPresent(joinHeartbeat -> executeAspect(SourceJoinHeartbeatAspect.class, () -> new SourceJoinHeartbeatAspect().dataProcessorContext(dataProcessorContext).joinHeartbeat(joinHeartbeat)));
				anyError = () -> {
					streamReadFunction.streamRead(getConnectorNode().getConnectorContext(), tables,
							syncProgress.getStreamOffsetObj(), increaseReadSize, streamReadConsumer);
				};
			}
		}

		if (null != anyError) {
			obsLogger.info("Starting stream read, table list: " + tables + ", offset: " + JSONUtil.obj2Json(syncProgress.getStreamOffsetObj()));

			CommonUtils.AnyError finalAnyError = anyError;
			String finalStreamReadFunctionName = streamReadFunctionName;
			executeDataFuncAspect(StreamReadFuncAspect.class, () -> new StreamReadFuncAspect()
							.connectorContext(connectorNode.getConnectorContext())
							.dataProcessorContext(getDataProcessorContext())
							.streamReadFunction(finalStreamReadFunctionName)
							.tables(tables)
							.eventBatchSize(increaseReadSize)
							.offsetState(syncProgress.getStreamOffsetObj())
							.start(),
					streamReadFuncAspect -> {
						this.streamReadFuncAspect = streamReadFuncAspect;
						PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_STREAM_READ, streamReadMethodInvoker.runnable(finalAnyError));
					});
		} else {
			throw new NodeException("PDK node does not support stream read: " + dataProcessorContext.getDatabaseType()).context(getProcessorBaseContext());
		}
	}

	protected StreamReadConsumer generateStreamReadConsumer(ConnectorNode connectorNode, PDKMethodInvoker pdkMethodInvoker) {
		return StreamReadConsumer.create((events, offsetObj) -> {
			try {
				while (isRunning()) {
					try {
						if (sourceRunnerLock.tryLock(1L, TimeUnit.SECONDS)) {
							break;
						}
					} catch (InterruptedException e) {
						break;
					}
				}
				if (events != null && !events.isEmpty()) {
					events = events.stream().map(event -> {
						if (null == event.getTime()) {
							throw new NodeException("Invalid TapEvent, `TapEvent.time` should be NonNUll").context(getProcessorBaseContext()).event(event);
						}
						return cdcDelayCalculation.filterAndCalcDelay(event, times -> AspectUtils.executeAspect(SourceCDCDelayAspect.class, () -> new SourceCDCDelayAspect().delay(times).dataProcessorContext(dataProcessorContext)),this.dataProcessorContext.getTaskDto().getSyncType());
					}).collect(Collectors.toList());

					if (streamReadFuncAspect != null) {
						AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_READ_COMPLETED).getStreamingReadCompleteConsumers(), events);
					}

					List<TapdataEvent> tapdataEvents = wrapTapdataEvent(events, SyncStage.CDC, offsetObj);
					if (logger.isDebugEnabled()) {
						logger.debug("Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(getConnectorNode()));
					}

					if (streamReadFuncAspect != null)
						AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_PROCESS_COMPLETED).getStreamingProcessCompleteConsumers(), tapdataEvents);

					if (CollectionUtils.isNotEmpty(tapdataEvents)) {
						tapdataEvents.forEach(this::enqueue);
						syncProgress.setStreamOffsetObj(offsetObj);
						if (streamReadFuncAspect != null)
							AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_ENQUEUED).getStreamingEnqueuedConsumers(), tapdataEvents);
						PDKInvocationMonitor.invokerRetrySetter(pdkMethodInvoker);
					}
				}
			} catch (Throwable throwable) {
				errorHandle(throwable, "Error processing incremental data, error: " + throwable.getMessage());
			} finally {
				try {
					sourceRunnerLock.unlock();
				} catch (Exception ignored) {
				}
			}
		}).stateListener((oldState, newState) -> {
			if (null != newState && StreamReadConsumer.STATE_STREAM_READ_STARTED == newState) {
				executeAspect(new CDCReadStartedAspect().dataProcessorContext(dataProcessorContext));
				// MILESTONE-READ_CDC_EVENT-FINISH
				if (streamReadFuncAspect != null)
					executeAspect(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAM_STARTED).streamStartedTime(System.currentTimeMillis()));
				sendCdcStartedEvent();
				PDKInvocationMonitor.invokerRetrySetter(pdkMethodInvoker);
				obsLogger.info("Connector {} incremental start succeed, tables: {}, data change syncing", connectorNode.getTapNodeInfo().getTapNodeSpecification().getName(), streamReadFuncAspect != null ? streamReadFuncAspect.getTables() : null);
			}
		});
	}

	private void handleSyncProgressType(List<TapdataEvent> tapdataEvents) {
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			if (sourceMode == SourceMode.NORMAL) {
				tapdataEvent.setType(syncProgressType);
			} else if (sourceMode == SourceMode.LOG_COLLECTOR) {
				tapdataEvent.setType(SyncProgress.Type.LOG_COLLECTOR);
			}
		}
	}

	private void sendCdcStartedEvent() {
		TapdataStartedCdcEvent tapdataStartedCdcEvent = TapdataStartedCdcEvent.create();
		tapdataStartedCdcEvent.setCdcStartTime(System.currentTimeMillis());
		tapdataStartedCdcEvent.setSyncStage(SyncStage.CDC);
		Node<?> node = getNode();
		if (node.isLogCollectorNode()) {
			List<Connections> connections = ShareCdcUtil.getConnectionIds(getNode(), ids -> {
				Query connectionQuery = new Query(where("_id").in(ids));
				connectionQuery.fields().include("config").include("pdkHash");
				return clientMongoOperator.find(connectionQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
			});
			if (CollectionUtils.isNotEmpty(connections)) {
				connections.forEach(connection -> {
					LogCollectorNode logCollectorNode = (LogCollectorNode) node;
					List<String> logNameSpaces = logCollectorNode.getLogCollectorConnConfigs().get(connection.getId()).getNamespace();
					logNameSpaces.forEach(logNameSpace -> {
						connection.getNamespace().forEach(nameSpace -> {
							if (logNameSpace.equals(nameSpace)) {
								TapdataStartedCdcEvent startedCdcEvent = TapdataStartedCdcEvent.create();
								startedCdcEvent.setCdcStartTime(System.currentTimeMillis());
								startedCdcEvent.setSyncStage(SyncStage.CDC);
								startedCdcEvent.setType(SyncProgress.Type.LOG_COLLECTOR);
								startedCdcEvent.addInfo(TapdataEvent.CONNECTION_ID_INFO_KEY, connection.getId());
								startedCdcEvent.addInfo(TapdataEvent.TABLE_NAMES_INFO_KEY, logCollectorNode.getLogCollectorConnConfigs().get(connection.getId()).getTableNames());
								enqueue(startedCdcEvent);
							}
						});
					});

				});
			} else {
				LogCollectorNode logCollectorNode = (LogCollectorNode) node;
				tapdataStartedCdcEvent.setType(SyncProgress.Type.LOG_COLLECTOR);
				tapdataStartedCdcEvent.addInfo(TapdataEvent.CONNECTION_ID_INFO_KEY, dataProcessorContext.getConnections().getId());
				tapdataStartedCdcEvent.addInfo(TapdataEvent.TABLE_NAMES_INFO_KEY, logCollectorNode.getTableNames());
				enqueue(tapdataStartedCdcEvent);
			}
		} else {
			enqueue(tapdataStartedCdcEvent);
		}
	}

	protected void doShareCdc() throws Exception {
		if (!isRunning()) {
			return;
		}
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		List<String> tables = new ArrayList<>(tapTableMap.keySet());
		excludeRemoveTable(tables);
		Optional.of(cdcDelayCalculation.addHeartbeatTable(tables))
				.map(joinHeartbeat -> executeAspect(SourceJoinHeartbeatAspect.class, () -> new SourceJoinHeartbeatAspect().dataProcessorContext(dataProcessorContext).joinHeartbeat(joinHeartbeat)));
		ShareCdcTaskContext shareCdcTaskContext = createShareCDCTaskContext();
		this.syncProgressType = SyncProgress.Type.SHARE_CDC;
		PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
		shareCdcTaskContext.setTableNames(tables);
		try {
			executeDataFuncAspect(StreamReadFuncAspect.class,
					() -> new StreamReadFuncAspect()
							.dataProcessorContext(getDataProcessorContext())
							.tables(tables)
							.eventBatchSize(1)
							.offsetState(syncProgress.getStreamOffsetObj())
							.start(),
					streamReadFuncAspect -> {
						PDKInvocationMonitor.invokerRetrySetter(pdkMethodInvoker);
						this.streamReadFuncAspect = streamReadFuncAspect;
						pdkMethodInvoker.runnable(()-> {
							// Init share cdc reader, if unavailable, will throw ShareCdcUnsupportedException
							this.shareCdcReader = ShareCdcFactory.shareCdcReader(ReaderType.PDK_TASK_HAZELCAST, shareCdcTaskContext, syncProgress.getStreamOffsetObj());
							obsLogger.info("Starting incremental sync, read from share log storage...");
							// Start listen message entity from share storage log
							this.shareCdcReader.listen(streamReadConsumer);
						});
						RetryUtils.autoRetry(PDKMethod.IENGINE_SHARE_CDC_LISTEN, pdkMethodInvoker);
					});
		} finally {
			Optional.ofNullable(pdkMethodInvoker).ifPresent(this::removePdkMethodInvoker);
		}
	}

	protected ShareCdcTaskContext createShareCDCTaskContext() {
		ShareCdcTaskContext shareCdcTaskContext = new ShareCdcTaskPdkContext(getCdcStartTs(), processorBaseContext.getConfigurationCenter(),
				dataProcessorContext.getTaskDto(), dataProcessorContext.getNode(), dataProcessorContext.getSourceConn(), getConnectorNode());
		shareCdcTaskContext.setObsLogger(obsLogger);
		return shareCdcTaskContext;
	}

	private void checkPollingCDCIfNeed() {
		Node node = getNode();
		if (!isPollingCDC(node)) {
			return;
		}
		if (!(node instanceof TableNode)) {
			throw new IllegalArgumentException(node.getClass().getSimpleName() + " not support polling cdc");
		}
		List<TableNode.CdcPollingField> cdcPollingFields = ((TableNode) node).getCdcPollingFields();
		if (CollectionUtils.isEmpty(cdcPollingFields)) {
			throw new IllegalArgumentException("Polling cdc must specify conditional field");
		}
		conditionFields = cdcPollingFields.stream().map(TableNode.CdcPollingField::getField).collect(Collectors.toList());
	}

	private void doPollingCDC() {
		if (!isRunning()) {
			return;
		}
		syncProgressType = SyncProgress.Type.POLLING_CDC;
		Node node = getNode();
		AtomicLong loopTime = new AtomicLong(1L);
		TableNode tableNode = (TableNode) node;
		String tableName = tableNode.getTableName();
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableName);
		Object streamOffsetObj = syncProgress.getStreamOffsetObj();
		if (!(streamOffsetObj instanceof Map)) {
			throw new IllegalArgumentException("Unrecognized polling cdc offset type, expecting: " + Map.class.getName() + ", actual: " + streamOffsetObj.getClass().getName());
		}
		Map<String, Object> tablePollingCDCOffset;
		tablePollingCDCOffset = getTablePollingCDCOffset((TableNode) node, tableName, tapTable, (Map) streamOffsetObj);
		long cdcPollingInterval = ((TableNode) node).getCdcPollingInterval();
		cdcPollingInterval = Math.max(cdcPollingInterval, CDC_POLLING_MIN_INTERVAL_MS);
		long logInterval = TimeUnit.MINUTES.toMillis(5);
		long logLoopTime = logInterval / cdcPollingInterval;
		long heartbeatInterval = TimeUnit.MINUTES.toMillis(1);
		long heartbeatTime = heartbeatInterval / cdcPollingInterval;
		int cdcPollingBatchSize = ((TableNode) node).getCdcPollingBatchSize();
		cdcPollingBatchSize = Math.max(cdcPollingBatchSize, CDC_POLLING_MIN_BATCH_SIZE);

		ConnectorNode connectorNode = getConnectorNode();
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();
		if (null == queryByAdvanceFilterFunction) {
			throw new RuntimeException("Node " + connectorNode + " not support query by advance filter, cannot do polling cdc");
		}
		String logMsg = "Start run table [" + tableName + "] polling cdc with parameters \n - Conditional field(s): " + streamOffsetObj;
		logMsg += "\n - Loop polling interval: " + cdcPollingInterval + " ms\n - Batch size: " + cdcPollingBatchSize;
		obsLogger.info(logMsg);
		while (isRunning()) {
			TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();
			for (Map.Entry<String, Object> entry : tablePollingCDCOffset.entrySet()) {
				String field = entry.getKey();
				Object value = entry.getValue();
				if (null != value) {
					tapAdvanceFilter.op(QueryOperator.gt(field, value));
				}
				tapAdvanceFilter.sort(SortOn.ascending(field));
			}
			if (isTableFilter(tableNode)) {
				List<QueryOperator> conditions = tableNode.getConditions();
				if (CollectionUtils.isNotEmpty(conditions)) {
					DataMap match = new DataMap();
					for (QueryOperator queryOperator : conditions) {
						if (EQUAL_VALUE == queryOperator.getOperator()) {
							match.put(queryOperator.getKey(), queryOperator.getValue());
						} else {
							tapAdvanceFilter.op(queryOperator);
						}
					}
					tapAdvanceFilter.match(match);
				}
			}
			tapAdvanceFilter.limit(cdcPollingBatchSize);
			try {
				if (loopTime.get() == 1L || loopTime.get() % logLoopTime == 0) {
					obsLogger.info("Query by advance filter\n - loop time: " + loopTime + "\n - table: " + tapTable.getId()
							+ "\n - filter: " + tapAdvanceFilter.getOperators()
							+ "\n - limit: " + tapAdvanceFilter.getLimit() + "\n - sort: " + tapAdvanceFilter.getSortOnList());
				}
				PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
				try {
				int finalCdcPollingBatchSize = cdcPollingBatchSize;
				AtomicBoolean hasData = new AtomicBoolean(false);
				executeDataFuncAspect(
						StreamReadFuncAspect.class,
						() -> new StreamReadFuncAspect()
								.dataProcessorContext(getDataProcessorContext())
								.tables(Collections.singletonList(tableName))
								.eventBatchSize(finalCdcPollingBatchSize)
								.offsetState(syncProgress.getStreamOffsetObj())
								.start(),
						streamReadFuncAspect -> PDKInvocationMonitor.invoke(
								getConnectorNode(), PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
								pdkMethodInvoker.runnable(() -> {
									Consumer<FilterResults> consumer = rs -> {
										List<Map<String, Object>> results = rs.getResults();
										if (CollectionUtils.isEmpty(results)) {
											return;
										}
										for (Map<String, Object> result : results) {
											hasData.compareAndSet(false, true);
											TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent
													.create()
													.after(result)
													.table(tableName)
													.referenceTime(System.currentTimeMillis())
													.init();
											if (streamReadFuncAspect != null) {
												AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_READ_COMPLETED).getStreamingReadCompleteConsumers(), Collections.singletonList(tapInsertRecordEvent));
											}
											flushPollingCDCOffset(tapInsertRecordEvent);
											TapdataEvent tapdataEvent = wrapTapdataEvent(tapInsertRecordEvent, SyncStage.CDC, syncProgress.getStreamOffsetObj(), true);
											if (null == tapdataEvent) {
												return;
											}
											List<TapdataEvent> tapdataEvents = Collections.singletonList(tapdataEvent);
											if (streamReadFuncAspect != null)
												AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_PROCESS_COMPLETED).getStreamingProcessCompleteConsumers(), tapdataEvents);
											tapdataEvent.setType(syncProgressType);
											enqueue(tapdataEvent);
											if (streamReadFuncAspect != null)
												AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_ENQUEUED).getStreamingEnqueuedConsumers(), tapdataEvents);
										}
									};
									queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, consumer);
								})
						));
				if (!hasData.get() && (loopTime.get() == 1L || loopTime.get() % heartbeatTime == 0)) {
					enqueue(TapdataHeartbeatEvent.create(System.currentTimeMillis(), syncProgress.getStreamOffsetObj(), SyncProgress.Type.POLLING_CDC));
				}
				}finally {
					removePdkMethodInvoker(pdkMethodInvoker);
				}
			} catch (Throwable e) {
				throw new RuntimeException("Query by advance filter failed, table: " + tapTable.getId() + ", filer: " + tapAdvanceFilter.getOperators() + ", sort: " + tapAdvanceFilter.getSortOnList() + ", limit: " + tapAdvanceFilter.getLimit(), e);
			}
			try {
				TimeUnit.MILLISECONDS.sleep(cdcPollingInterval);
			} catch (InterruptedException e) {
				break;
			}
			loopTime.incrementAndGet();
		}
	}

	private Map<String, Object> getTablePollingCDCOffset(TableNode node, String tableName, TapTable tapTable, Map streamOffsetObj) {
		Map<String, Object> tablePollingCDCOffset;
		if (MapUtils.isEmpty(streamOffsetObj) || !streamOffsetObj.containsKey(tableName)) {
			tablePollingCDCOffset = new HashMap<>();
			List<TableNode.CdcPollingField> cdcPollingFields = node.getCdcPollingFields();
			for (TableNode.CdcPollingField cdcPollingField : cdcPollingFields) {
				String field = cdcPollingField.getField();
				String defaultValue = cdcPollingField.getDefaultValue();
				if (syncType.equals(SyncTypeEnum.CDC) && StringUtils.isEmpty(defaultValue)) {
					throw new IllegalArgumentException("Polling cdc conditional field [" + field + "] must set a start default value");
				}
				TapField tapField = tapTable.getNameFieldMap().get(field);
				TapType tapType = tapField.getTapType();
				Object convertValue = defaultValue;
				if (null != convertValue) {
					convertValue = getConvertValue(tapType, defaultValue);
				}
				tablePollingCDCOffset.put(field, convertValue);
			}
			streamOffsetObj.put(tableName, tablePollingCDCOffset);
		} else {
			tablePollingCDCOffset = (Map<String, Object>) streamOffsetObj.get(tableName);
		}
		return tablePollingCDCOffset;
	}

	private Object getConvertValue(TapType tapType, String defaultValue) {
		Object convertValue = defaultValue;
		switch (tapType.getType()) {
			case TapType.TYPE_NUMBER:
				if (defaultValue.contains(".")) {
					try {
						convertValue = Double.valueOf(defaultValue);
					} catch (NumberFormatException e) {
						throw new RuntimeException("Convert polling cdc condition value [" + defaultValue + "] to Double failed", e);
					}
				} else {
					try {
						convertValue = Long.valueOf(defaultValue);
					} catch (NumberFormatException e) {
						throw new RuntimeException("Convert polling cdc condition value [" + defaultValue + "] to Long failed", e);
					}
				}
				break;
			case TapType.TYPE_DATE:
				LocalDate localDate;
				String dateFormat = "yyyy-MM-dd";
				try {
					localDate = LocalDate.parse(defaultValue, DateTimeFormatter.ofPattern(dateFormat));
				} catch (Exception e) {
					throw new RuntimeException("Convert polling cdc condition value [" + defaultValue + "] to LocalDate failed, format: " + dateFormat);
				}
				ZonedDateTime gmtZonedDate = localDate.atStartOfDay(ZoneId.of("GMT"));
				convertValue = new DateTime(gmtZonedDate);
				break;
			case TapType.TYPE_DATETIME:
				LocalDateTime localDateTime;
				String datetimeFormat = "yyyy-MM-dd HH:mm:ss";
				try {
					localDateTime = LocalDateTime.parse(defaultValue, DateTimeFormatter.ofPattern(datetimeFormat));
				} catch (Exception e) {
					throw new RuntimeException("The input string format is incorrect, expected format: " + datetimeFormat + ", actual value: " + defaultValue);
				}
				ZonedDateTime gmtZonedDateTime = localDateTime.atZone(ZoneId.of("GMT"));
				convertValue = new DateTime(gmtZonedDateTime);
				break;
			default:
				break;
		}
		return convertValue;
	}

	protected void flushPollingCDCOffset(List<TapEvent> tapEvents) {
		if (CollectionUtils.isEmpty(tapEvents)) {
			return;
		}
		TapEvent lastEvent = tapEvents.get(tapEvents.size() - 1);
		flushPollingCDCOffset(lastEvent);
	}

	protected void flushPollingCDCOffset(TapEvent tapEvent) {
		if (!isPollingCDC(getNode())) {
			return;
		}
		if (!(tapEvent instanceof TapRecordEvent)) {
			return;
		}
		TableNode node = (TableNode) getNode();
		String tableName = node.getTableName();
		Map streamOffsetMap = (Map) syncProgress.getStreamOffsetObj();
		if (!streamOffsetMap.containsKey(tableName)) {
			streamOffsetMap.put(tableName, new HashMap<>());
		}
		Map tablePollingCDCOffset = (Map) streamOffsetMap.get(tableName);
		Map<String, Object> data;
		if (tapEvent instanceof TapInsertRecordEvent) {
			data = ((TapInsertRecordEvent) tapEvent).getAfter();
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			data = ((TapUpdateRecordEvent) tapEvent).getAfter();
		} else {
			data = ((TapDeleteRecordEvent) tapEvent).getBefore();
		}
		for (String conditionField : conditionFields) {
			Object value = data.get(conditionField);
			tablePollingCDCOffset.put(conditionField, value);
		}
		TapCodecsFilterManager connectorCodecsFilterManger = getConnectorNode().getCodecsFilterManager();
		toTapValue(tablePollingCDCOffset, ((TapRecordEvent)tapEvent).getTableId(), connectorCodecsFilterManger);
		fromTapValue(tablePollingCDCOffset, this.defaultCodecsFilterManager, getTgtTableNameFromTapEvent(tapEvent));
	}

	private Long getCdcStartTs() {
		Long cdcStartTs = null;
		try {
			if (null != this.syncProgress && null != this.syncProgress.getEventTime() && this.syncProgress.getEventTime().compareTo(0L) > 0) {
				cdcStartTs = this.syncProgress.getEventTime();
			}
		} catch (Exception e) {
			throw new NodeException("Get cdc start ts failed; Error: " + e.getMessage(), e).context(getProcessorBaseContext());
		}
		return cdcStartTs;
	}

	@Override
	public void doClose() throws TapCodeException {
		try {
			CommonUtils.handleAnyError(() -> {
				if (null != shareCdcReader) {
					shareCdcReader.close();
				}
			}, err -> obsLogger.warn(String.format("Close share cdc log reader failed: %s", err.getMessage())));
		} finally {
			super.doClose();
		}
	}

	private TapAdvanceFilter batchFilterRead() {
		TableNode tableNode = (TableNode) dataProcessorContext.getNode();
		TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
		if (isTableFilter(tableNode)) {
			List<QueryOperator> conditions = timeTransformation(tableNode.getConditions(),tableNode.getOffsetHours());
			if (CollectionUtils.isNotEmpty(conditions)) {
				String tableName = tableNode.getTableName();
				TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableName);
				DataMap match = new DataMap();
				List<QueryOperator> queryOperators = new ArrayList<>();
				for (QueryOperator queryOperator : conditions) {
					TapField tapField = tapTable.getNameFieldMap().get(queryOperator.getKey());
					TapType tapType = tapField.getTapType();
					Object convertValue;
					if (queryOperator.getValue() != null) {
						if(queryOperator.getOriginalValue() == null) {
							convertValue = getConvertValue(tapType, queryOperator.getValue().toString());
							queryOperator.setOriginalValue(queryOperator.getValue().toString());
						}else {
							convertValue = getConvertValue(tapType, queryOperator.getOriginalValue().toString());
						}
						queryOperator.setValue(convertValue);
					}
					if (EQUAL_VALUE == queryOperator.getOperator()) {
						match.put(queryOperator.getKey(), queryOperator.getValue());
					} else {
						queryOperators.add(queryOperator);
					}
				}
				tapAdvanceFilter.setMatch(match);
				tapAdvanceFilter.setOperators(queryOperators);
			}
			Integer limit = tableNode.getLimit();
			if (null != limit) {
				tapAdvanceFilter.setLimit(limit);
			}
		}

		if (isPollingCDC(tableNode)) {
			List<TableNode.CdcPollingField> cdcPollingFields = tableNode.getCdcPollingFields();
			if (CollectionUtils.isNotEmpty(cdcPollingFields)) {
				for (TableNode.CdcPollingField cdcPollingField : cdcPollingFields) {
					tapAdvanceFilter.sort(SortOn.ascending(cdcPollingField.getField()));
				}
			}
		}
		return tapAdvanceFilter;
	}

	protected List<QueryOperator> timeTransformation(List<QueryOperator> conditions,Long offsetHours){
		List<QueryOperator> finalConditions = new ArrayList<>();
		for(QueryOperator queryOperator : conditions){
			LocalDateTime currentDateTime = LocalDateTime.now();
			if(queryOperator.isFastQuery()){
				List<String> timeList = TimeTransFormationUtil.calculatedTimeRange(currentDateTime,queryOperator,offsetHours);
				List<QueryOperator> result = constructQueryOperator(timeList,queryOperator);
				if(CollectionUtils.isNotEmpty(result))finalConditions.addAll(result);
			}else{
				finalConditions.add(queryOperator);
			}
		}
		return finalConditions;
	}

	protected List<QueryOperator> constructQueryOperator(List<String> timeList,QueryOperator queryOperator){
		List<QueryOperator> result = new ArrayList<>();
		if(CollectionUtils.isEmpty(timeList))return result;
		QueryOperator start = new QueryOperator();
		start.setKey(queryOperator.getKey());
		start.setOperator(QueryOperator.GTE);
		start.setValue(timeList.get(0));
		QueryOperator end = new QueryOperator();
		end.setKey(queryOperator.getKey());
		end.setOperator(QueryOperator.LTE);
		end.setValue(timeList.get(1));
		result.add(start);
		result.add(end);
		return result;
	}


	protected void excludeRemoveTable(List<String> tableNames) {
		if (null == tableNames) {
			return;
		}
		if (CollectionUtils.isEmpty(removeTables)) {
			return;
		}
		tableNames.removeAll(removeTables);
	}

	@Override
	protected Long doBatchCountFunction(BatchCountFunction batchCountFunction, TapTable table) {
		AtomicReference<Long> counts = new AtomicReference<>();

		executeDataFuncAspect(
				TableCountFuncAspect.class,
				() -> new TableCountFuncAspect().dataProcessorContext(getDataProcessorContext()).start(),
				tableCountFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_BATCH_COUNT, createPdkMethodInvoker().runnable(() -> {
					try {
						Node<?> node = getNode();
						if (node instanceof TableNode) {
							TableNode tableNode = (TableNode) dataProcessorContext.getNode();
							if (isTableFilter(tableNode) || isPollingCDC(tableNode)) {
								ConnectorNode connectorNode = getConnectorNode();
								ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
								CountByPartitionFilterFunction countByPartitionFilterFunction = connectorFunctions.getCountByPartitionFilterFunction();
								if (countByPartitionFilterFunction == null) {
									counts.set(batchCountFunction.count(getConnectorNode().getConnectorContext(), table));
								}else {
									TapAdvanceFilter tapAdvanceFilter = batchFilterRead();
									counts.set(countByPartitionFilterFunction.countByPartitionFilter(connectorNode.getConnectorContext(), table, tapAdvanceFilter));
								}
							} else {
								counts.set(batchCountFunction.count(getConnectorNode().getConnectorContext(), table));
							}
						} else {
							counts.set(batchCountFunction.count(getConnectorNode().getConnectorContext(), table));
						}


						if (null != tableCountFuncAspect) {
							AspectUtils.accept(tableCountFuncAspect.state(TableCountFuncAspect.STATE_COUNTING).getTableCountConsumerList(), table.getName(), counts.get());
						}
					} catch (Throwable e) {
						throw new NodeException("Query table '" + table.getName() + "'  count failed: " + e.getMessage(), e)
								.context(getProcessorBaseContext());
					}
				}))
		);
		return counts.get();
	}

}

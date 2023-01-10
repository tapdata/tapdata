package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataCompleteSnapshotEvent;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataStartCdcEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.sharecdc.ReaderType;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcReader;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.flow.engine.V2.sharecdc.impl.ShareCdcFactory;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * @author jackin
 * @date 2022/2/22 2:33 PM
 **/
public class HazelcastSourcePdkDataNode extends HazelcastSourcePdkBaseNode {
	private static final String TAG = HazelcastSourcePdkDataNode.class.getSimpleName();
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkDataNode.class);

	private static final int ASYNCLY_COUNT_SNAPSHOT_ROW_SIZE_TABLE_THRESHOLD = 100;

	private static final int EQUAL_VALUE = 5;


	private ShareCdcReader shareCdcReader;

	private final SourceStateAspect sourceStateAspect;
	private Map<String, Long> snapshotRowSizeMap;
	private ExecutorService snapshotRowSizeThreadPool;

	public HazelcastSourcePdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		sourceStateAspect = new SourceStateAspect().dataProcessorContext(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		try {
			super.doInit(context);
			// MILESTONE-INIT_CONNECTOR-FINISH
			TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.FINISH);
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.FINISH);
		} catch (Throwable e) {
			// MILESTONE-INIT_CONNECTOR-ERROR
			TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.ERROR, logger);
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			//Notify error for task.
			throw errorHandle(e, "init failed");
		}
	}

	@Override
	public void startSourceRunner() {
		try {
			Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
			TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
			try {
				if (need2InitialSync(syncProgress)) {
					if (this.sourceRunnerFirstTime.get()) {
						doSnapshot(new ArrayList<>(tapTableMap.keySet()));
					}
				}
				if (!sourceRunnerFirstTime.get() && CollectionUtils.isNotEmpty(newTables)) {
					doSnapshot(newTables);
				}
			} catch (Throwable e) {
				TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.ERROR, logger);
				MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
				throw e;
			} finally {
				Optional.ofNullable(snapshotProgressManager).ifPresent(SnapshotProgressManager::close);
			}
			if (need2CDC()) {
				try {
					AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_CDC_START));
					doCdc();
				} catch (Throwable e) {
					// MILESTONE-READ_CDC_EVENT-ERROR
					TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.ERROR);
					MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					throw e;
				} finally {
					AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_CDC_COMPLETED));
				}
			}
		} catch (Throwable throwable) {
			errorHandle(throwable, throwable.getMessage());
		}
	}

	@SneakyThrows
	private void doSnapshot(List<String> tableList) {
		syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());

		// count the data size of the tables;
		doCount(tableList);

		BatchReadFunction batchReadFunction = getConnectorNode().getConnectorFunctions().getBatchReadFunction();
		QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = getConnectorNode().getConnectorFunctions().getQueryByAdvanceFilterFunction();

		if (batchReadFunction != null) {
			// MILESTONE-READ_SNAPSHOT-RUNNING
			if (sourceRunnerFirstTime.get()) {
				AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_START));
				TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.RUNNING);
			}
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.RUNNING);
			try {
				while (isRunning()) {
					for (String tableName : tableList) {
						// wait until we count the table
						while (isRunning() && (null == snapshotRowSizeMap || !snapshotRowSizeMap.containsKey(tableName))) {
							try {
								TimeUnit.MILLISECONDS.sleep(500);
							} catch (InterruptedException ignored) {
							}
						}
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
							if (!isRunning()) {
								break;
							}
							if (this.removeTables != null && this.removeTables.contains(tableName)) {
								logger.info("Table " + tableName + " is detected that it has been removed, the snapshot read will be skipped");
								obsLogger.info("Table " + tableName + " is detected that it has been removed, the snapshot read will be skipped");
								this.removeTables.remove(tableName);
								continue;
							}
							TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableName);
							Object tableOffset = ((Map<String, Object>) syncProgress.getBatchOffsetObj()).get(tapTable.getId());
							logger.info("Starting batch read, table name: " + tapTable.getId() + ", offset: " + tableOffset);
							obsLogger.info("Starting batch read, table name: " + tapTable.getId() + ", offset: " + tableOffset);

							executeDataFuncAspect(
									BatchReadFuncAspect.class, () -> new BatchReadFuncAspect()
											.eventBatchSize(batchSize)
											.connectorContext(getConnectorNode().getConnectorContext())
											.offsetState(tableOffset)
											.dataProcessorContext(this.getDataProcessorContext())
											.start()
											.table(tapTable),
									batchReadFuncAspect -> PDKInvocationMonitor.invoke(
											getConnectorNode(), PDKMethod.SOURCE_BATCH_READ,
											createPdkMethodInvoker().runnable(() -> {
														BiConsumer<List<TapEvent>, Object> consumer = (events, offsetObject) -> {
															if (events != null && !events.isEmpty()) {
																events.forEach(event -> {
																	if (null == event.getTime()) {
																		throw new NodeException("Invalid TapEvent, `TapEvent.time` should be NonNUll").context(getProcessorBaseContext()).event(event);
																	}
																	event.addInfo("eventId", UUID.randomUUID().toString());
																});

																if (batchReadFuncAspect != null)
																	AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_READ_COMPLETE).getReadCompleteConsumers(), events);

																if (logger.isDebugEnabled()) {
																	logger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(getConnectorNode()));
																}
																((Map<String, Object>) syncProgress.getBatchOffsetObj()).put(tapTable.getId(), offsetObject);
																List<TapdataEvent> tapdataEvents = wrapTapdataEvent(events);

																if (batchReadFuncAspect != null)
																	AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_PROCESS_COMPLETE).getProcessCompleteConsumers(), tapdataEvents);

																if (CollectionUtil.isNotEmpty(tapdataEvents)) {
																	tapdataEvents.forEach(this::enqueue);

																	if (batchReadFuncAspect != null)
																		AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_ENQUEUED).getEnqueuedConsumers(), tapdataEvents);
																}
															}
														};
														if (getNode() instanceof TableNode) {
															TableNode tableNode = (TableNode) dataProcessorContext.getNode();
															if (tableNode.getIsFilter() && CollectionUtils.isNotEmpty(tableNode.getConditions())) {
																TapAdvanceFilter tapAdvanceFilter = batchFilterRead();
																queryByAdvanceFilterFunction.query(getConnectorNode().getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {
																	List<TapEvent> tempList = new ArrayList<>();
																	if (filterResults != null && CollectionUtils.isNotEmpty(filterResults.getResults())) {
																		filterResults.getResults().forEach(filterResult -> tempList.add(TapSimplify.insertRecordEvent(filterResult, tapTable.getId())));
																	}
																	if (CollectionUtils.isNotEmpty(tempList)) {
																		consumer.accept(tempList, null);
																		tempList.clear();
																	}
																});
															} else {
																batchReadFunction.batchRead(getConnectorNode().getConnectorContext(), tapTable, tableOffset, batchSize, consumer);
															}
														} else {
															batchReadFunction.batchRead(getConnectorNode().getConnectorContext(), tapTable, tableOffset, batchSize, consumer);
														}
													}
											)
									));
						} catch (Throwable throwable) {
							Throwable throwableWrapper = throwable;
							if (!(throwableWrapper instanceof NodeException)) {
								throwableWrapper = new NodeException(throwableWrapper).context(getProcessorBaseContext());
							}
							throw throwableWrapper;
						} finally {
							try {
								sourceRunnerLock.unlock();
							} catch (Exception ignored) {
							}
						}
					}
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
						if (CollectionUtils.isNotEmpty(newTables)) {
							tableList.clear();
							tableList.addAll(newTables);
							doCount(tableList);
							newTables.clear();
						} else {
							this.endSnapshotLoop.set(true);
							break;
						}
					} finally {
						try {
							sourceRunnerLock.unlock();
						} catch (Exception ignored) {
						}
					}
				}
			} finally {
				if (isRunning()) {
					// MILESTONE-READ_SNAPSHOT-FINISH
					TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.FINISH);
					MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.FINISH);
					enqueue(new TapdataCompleteSnapshotEvent());
					AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED));
				}
			}
		} else {
			throw new NodeException("PDK node does not support batch read: " + dataProcessorContext.getDatabaseType())
					.context(getProcessorBaseContext());
		}
	}

	@SneakyThrows
	private void doCount(List<String> tableList) {
		BatchCountFunction batchCountFunction = getConnectorNode().getConnectorFunctions().getBatchCountFunction();
		if (null == batchCountFunction) {
			setDefaultRowSizeMap();
			logger.warn("PDK node does not support table batch count: " + dataProcessorContext.getDatabaseType());
			obsLogger.warn("PDK node does not support table batch count: " + dataProcessorContext.getDatabaseType());
			return;
		}

		if (dataProcessorContext.getTapTableMap().keySet().size() > ASYNCLY_COUNT_SNAPSHOT_ROW_SIZE_TABLE_THRESHOLD) {
			logger.info("Start to asynchronously count the size of rows for the source table(s)");
			AtomicReference<TaskDto> task = new AtomicReference<>(dataProcessorContext.getTaskDto());
			AtomicReference<Node<?>> node = new AtomicReference<>(dataProcessorContext.getNode());
			snapshotRowSizeThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
			CompletableFuture.runAsync(() -> {
						String name = String.format("Snapshot-Row-Size-Query-Thread-%s(%s)-%s(%s)",
								task.get().getName(), task.get().getId().toHexString(), node.get().getName(), node.get().getId());
						Thread.currentThread().setName(name);
						Log4jUtil.setThreadContext(task.get());

						doCountSynchronously(batchCountFunction, tableList);
					}, snapshotRowSizeThreadPool)
					.whenComplete((v, e) -> {
						if (null != e) {
							logger.warn("Query snapshot row size failed: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
							obsLogger.warn("Query snapshot row size failed: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
						} else {
							logger.info("Query snapshot row size completed: " + node.get().getName() + "(" + node.get().getId() + ")");
							obsLogger.info("Query snapshot row size completed: " + node.get().getName() + "(" + node.get().getId() + ")");
						}
						ExecutorUtil.shutdown(this.snapshotRowSizeThreadPool, 10L, TimeUnit.SECONDS);
					});
		} else {
			doCountSynchronously(batchCountFunction, tableList);
		}
	}

	@SneakyThrows
	private void doCountSynchronously(BatchCountFunction batchCountFunction, List<String> tableList) {
		if (null == batchCountFunction) {
			setDefaultRowSizeMap();
			logger.warn("PDK node does not support table batch count: " + dataProcessorContext.getDatabaseType());
			obsLogger.warn("PDK node does not support table batch count: " + dataProcessorContext.getDatabaseType());
			return;
		}

		for (String tableName : tableList) {
			if (!isRunning()) {
				return;
			}

			TapTable table = dataProcessorContext.getTapTableMap().get(tableName);
			executeDataFuncAspect(TableCountFuncAspect.class, () -> new TableCountFuncAspect()
							.dataProcessorContext(this.getDataProcessorContext())
							.start(),
					tableCountFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_BATCH_COUNT,
							createPdkMethodInvoker().runnable(
									() -> {
										try {
											long count = batchCountFunction.count(getConnectorNode().getConnectorContext(), table);

											if (null == snapshotRowSizeMap) {
												snapshotRowSizeMap = new HashMap<>();
											}
											snapshotRowSizeMap.putIfAbsent(tableName, count);

											if (null != tableCountFuncAspect) {
												AspectUtils.accept(tableCountFuncAspect.state(TableCountFuncAspect.STATE_COUNTING).getTableCountConsumerList(), table.getName(), count);
											}
										} catch (Exception e) {
											throw new NodeException("Count " + table.getId() + " failed: " + e.getMessage(), e)
													.context(getProcessorBaseContext());
										}
									}
							)
					));
		}
	}

	@SneakyThrows
	private void doCdc() {
		if (!isRunning()) {
			return;
		}
		this.endSnapshotLoop.set(true);
		if (null == syncProgress.getStreamOffsetObj()) {
			throw new NodeException("Starting stream read failed, errors: start point offset is null").context(getProcessorBaseContext());
		} else {
			TapdataStartCdcEvent tapdataStartCdcEvent = new TapdataStartCdcEvent();
			tapdataStartCdcEvent.setSyncStage(SyncStage.CDC);
			enqueue(tapdataStartCdcEvent);
		}
		// MILESTONE-READ_CDC_EVENT-RUNNING
		TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.RUNNING);
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.RUNNING);
		syncProgress.setSyncStage(SyncStage.CDC.name());
		Node<?> node = dataProcessorContext.getNode();
		if (node.isLogCollectorNode()) {
			// Mining tasks force traditional increments
			doNormalCDC();
		} else {
			try {
				// Try to start with share cdc
				doShareCdc();
			} catch (ShareCdcUnsupportedException e) {
				if (e.isContinueWithNormalCdc()) {
					// If share cdc is unavailable, and continue with normal cdc is true
					logger.info("Share cdc unusable, will use normal cdc mode, reason: " + e.getMessage());
					obsLogger.info("Share cdc unusable, will use normal cdc mode, reason: " + e.getMessage());
					doNormalCDC();
				} else {
					throw new NodeException("Read share cdc log failed: " + e.getMessage(), e).context(getProcessorBaseContext());
				}
			} catch (Exception e) {
				throw new NodeException("Read share cdc log failed: " + e.getMessage(), e).context(getProcessorBaseContext());
			}
		}
	}

	@SneakyThrows
	private void doNormalCDC() {
		if (!isRunning()) {
			return;
		}
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		ConnectorNode connectorNode = getConnectorNode();
		if (connectorNode == null) {
			logger.warn("Failed to get source node");
			return;
		}
		RawDataCallbackFilterFunction rawDataCallbackFilterFunction = connectorNode.getConnectorFunctions().getRawDataCallbackFilterFunction();
		RawDataCallbackFilterFunctionV2 rawDataCallbackFilterFunctionV2 = connectorNode.getConnectorFunctions().getRawDataCallbackFilterFunctionV2();
//		if(rawDataCallbackFilterFunctionV2 != null) {
//			rawDataCallbackFilterFunction = null;
//		}
		StreamReadFunction streamReadFunction = connectorNode.getConnectorFunctions().getStreamReadFunction();
		if (streamReadFunction != null || rawDataCallbackFilterFunction != null || rawDataCallbackFilterFunctionV2 != null) {
			logger.info("Starting stream read, table list: " + tapTableMap.keySet() + ", offset: " + syncProgress.getStreamOffsetObj());
			List<String> tables = new ArrayList<>(tapTableMap.keySet());
			cdcDelayCalculation.addHeartbeatTable(tables);
			int batchSize = 1;
			String streamReadFunctionName = null;
			if (rawDataCallbackFilterFunctionV2 != null)
				streamReadFunctionName = rawDataCallbackFilterFunctionV2.getClass().getSimpleName();
			if (rawDataCallbackFilterFunction != null && streamReadFunctionName == null)
				streamReadFunctionName = rawDataCallbackFilterFunction.getClass().getSimpleName();
			if (streamReadFunctionName == null)
				streamReadFunctionName = streamReadFunction.getClass().getSimpleName();
			String finalStreamReadFunctionName = streamReadFunctionName;
			PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
			executeDataFuncAspect(StreamReadFuncAspect.class, () -> new StreamReadFuncAspect()
							.connectorContext(getConnectorNode().getConnectorContext())
							.dataProcessorContext(getDataProcessorContext())
							.streamReadFunction(finalStreamReadFunctionName)
							.tables(tables)
							.eventBatchSize(batchSize)
							.offsetState(syncProgress.getStreamOffsetObj())
							.start(),
					streamReadFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_STREAM_READ,
							pdkMethodInvoker.runnable(
									() -> {
										this.streamReadFuncAspect = streamReadFuncAspect;
										StreamReadConsumer streamReadConsumer = StreamReadConsumer.create((events, offsetObj) -> {
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
													events.forEach(event -> {
														if (null == event.getTime()) {
															throw new NodeException("Invalid TapEvent, `TapEvent.time` should be NonNUll").context(getProcessorBaseContext()).event(event);
														}
														event.addInfo("eventId", UUID.randomUUID().toString());
													});

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
											if (StreamReadConsumer.STATE_STREAM_READ_ENDED != newState) {
												PDKInvocationMonitor.invokerRetrySetter(pdkMethodInvoker);
											}
											if (null != newState && StreamReadConsumer.STATE_STREAM_READ_STARTED == newState) {
												// MILESTONE-READ_CDC_EVENT-FINISH
												if (streamReadFuncAspect != null)
													executeAspect(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAM_STARTED).streamStartedTime(System.currentTimeMillis()));
												TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.FINISH);
												MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.FINISH);
												logger.info("Connector start stream read succeed: {}", connectorNode);
												obsLogger.info("Connector start stream read succeed: {}", connectorNode);
											}
										});

										if ((rawDataCallbackFilterFunction != null || rawDataCallbackFilterFunctionV2 != null) && streamReadFuncAspect != null) {
											executeAspect(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_CALLBACK_RAW_DATA).streamReadConsumer(streamReadConsumer));
											while (isRunning()) {
												if (!streamReadFuncAspect.waitRawData()) {
													break;
												}
											}
											if (streamReadFuncAspect.getErrorDuringWait() != null) {
												throw streamReadFuncAspect.getErrorDuringWait();
											}
										} else {
											if (streamReadFunction != null) {
												streamReadFunction.streamRead(getConnectorNode().getConnectorContext(), tables,
														syncProgress.getStreamOffsetObj(), batchSize, streamReadConsumer);
											}
										}
									}
							)
					));
		} else {
			throw new NodeException("PDK node does not support stream read: " + dataProcessorContext.getDatabaseType()).context(getProcessorBaseContext());
		}
	}

	private void doShareCdc() throws Exception {
		if (!isRunning()) {
			return;
		}
		cdcDelayCalculation.addHeartbeatTable(new ArrayList<>(dataProcessorContext.getTapTableMap().keySet()));
		ShareCdcTaskContext shareCdcTaskContext = new ShareCdcTaskPdkContext(getCdcStartTs(), processorBaseContext.getConfigurationCenter(),
				dataProcessorContext.getTaskDto(), dataProcessorContext.getNode(), dataProcessorContext.getSourceConn(), getConnectorNode());
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		List<String> tables = new ArrayList<>(tapTableMap.keySet());
		// Init share cdc reader, if unavailable, will throw ShareCdcUnsupportedException
		this.shareCdcReader = ShareCdcFactory.shareCdcReader(ReaderType.PDK_TASK_HAZELCAST, shareCdcTaskContext);
		logger.info("Starting incremental sync, read from share log storage...");
		obsLogger.info("Starting incremental sync, read from share log storage...");
		// Start listen message entity from share storage log
		executeDataFuncAspect(StreamReadFuncAspect.class,
				() -> new StreamReadFuncAspect()
						.dataProcessorContext(getDataProcessorContext())
						.tables(tables)
						.eventBatchSize(1)
						.offsetState(syncProgress.getStreamOffsetObj())
						.start(),
				streamReadFuncAspect -> this.shareCdcReader.listen((event, offsetObj) -> {
					if (streamReadFuncAspect != null) {
						AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_READ_COMPLETED).getStreamingReadCompleteConsumers(), Collections.singletonList(event));
					}
					TapdataEvent tapdataEvent = wrapTapdataEvent(event, SyncStage.CDC, offsetObj, true);
					if (null == tapdataEvent) {
						return;
					}
					List<TapdataEvent> tapdataEvents = Collections.singletonList(tapdataEvent);
					if (streamReadFuncAspect != null)
						AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_PROCESS_COMPLETED).getStreamingProcessCompleteConsumers(), tapdataEvents);
					tapdataEvent.setType(SyncProgress.Type.SHARE_CDC);
					enqueue(tapdataEvent);
					if (streamReadFuncAspect != null)
						AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_ENQUEUED).getStreamingEnqueuedConsumers(), tapdataEvents);
				}));
	}

	private Long getCdcStartTs() {
		Long cdcStartTs;
		try {
			if (null != this.syncProgress && null != this.syncProgress.getEventTime() && this.syncProgress.getEventTime().compareTo(0L) > 0) {
				cdcStartTs = this.syncProgress.getEventTime();
			} else {
				cdcStartTs = initialFirstStartTime;
			}
		} catch (Exception e) {
			throw new NodeException("Get cdc start ts failed; Error: " + e.getMessage(), e).context(getProcessorBaseContext());
		}
		return cdcStartTs;
	}

	private void setDefaultRowSizeMap() {
		for (String tableName : dataProcessorContext.getTapTableMap().keySet()) {
			if (null == snapshotRowSizeMap) {
				snapshotRowSizeMap = new HashMap<>();
			}
			snapshotRowSizeMap.putIfAbsent(tableName, 0L);
		}
	}

	@Override
	public void doClose() throws Exception {
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
		TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
		DataMap match = new DataMap();
		TableNode tableNode = (TableNode) dataProcessorContext.getNode();
		List<QueryOperator> queryOperators = new ArrayList<>();
		for (QueryOperator queryOperator : tableNode.getConditions()) {
			if (EQUAL_VALUE == queryOperator.getOperator()) {
				match.put(queryOperator.getKey(), queryOperator.getValue());
			} else {
				queryOperators.add(queryOperator);
			}
		}
		tapAdvanceFilter.setMatch(match);
		tapAdvanceFilter.setOperators(queryOperators);
		tapAdvanceFilter.setLimit(tableNode.getLimit());
		return tapAdvanceFilter;
	}

}

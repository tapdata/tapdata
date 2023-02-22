package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataCompleteSnapshotEvent;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.entity.TapdataStartCdcEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.SourceStateAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TaskMilestoneFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.sharecdc.ReaderType;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcReader;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.flow.engine.V2.sharecdc.impl.ShareCdcFactory;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.RawDataCallbackFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.RawDataCallbackFilterFunctionV2;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author jackin
 * @date 2022/2/22 2:33 PM
 **/
public class HazelcastSourcePdkDataNode extends HazelcastSourcePdkBaseNode {
	private static final String TAG = HazelcastSourcePdkDataNode.class.getSimpleName();
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkDataNode.class);
	private static final int CDC_POLLING_MIN_INTERVAL_MS = 500;
	private static final int CDC_POLLING_MIN_BATCH_SIZE = 1000;
	private static final int EQUAL_VALUE = 5;
	private ShareCdcReader shareCdcReader;
	private final SourceStateAspect sourceStateAspect;
	private List<String> conditionFields;

	public HazelcastSourcePdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		sourceStateAspect = new SourceStateAspect().dataProcessorContext(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		try {
			super.doInit(context);
			checkPollingCDCIfNeed();
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
			} else {
				BeanUtil.getBean(TapdataTaskScheduler.class).getTaskClient(dataProcessorContext.getTaskDto().getId().toHexString()).terminalMode(TerminalMode.COMPLETE);
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
							int eventBatchSize = 512;

							executeDataFuncAspect(
									BatchReadFuncAspect.class, () -> new BatchReadFuncAspect()
											.eventBatchSize(readBatchSize)
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
																	event.addInfo(TAPEVENT_INFO_EVENT_ID_KEY, UUID.randomUUID().toString());
																});

																if (batchReadFuncAspect != null)
																	AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_READ_COMPLETE).getReadCompleteConsumers(), events);

																if (logger.isDebugEnabled()) {
																	logger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(getConnectorNode()));
																}
																((Map<String, Object>) syncProgress.getBatchOffsetObj()).put(tapTable.getId(), offsetObject);
																flushPollingCDCOffset(events);
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
															if (isTableFilter(tableNode) || isPollingCDC(tableNode)) {
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
																batchReadFunction.batchRead(getConnectorNode().getConnectorContext(), tapTable, tableOffset, readBatchSize, consumer);
															}
														} else {
															batchReadFunction.batchRead(getConnectorNode().getConnectorContext(), tapTable, tableOffset, readBatchSize, consumer);
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

	private static boolean isTableFilter(TableNode tableNode) {
		return tableNode.getIsFilter() && CollectionUtils.isNotEmpty(tableNode.getConditions());
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
		if (isPollingCDC(node)) {
			doPollingCDC();
		} else {
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
	}

	@SneakyThrows
	protected void doNormalCDC() {
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
			obsLogger.info("Starting stream read, table list: " + tapTableMap.keySet() + ", offset: " + syncProgress.getStreamOffsetObj());
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
		Node node = getNode();
		AtomicLong loopTime = new AtomicLong(1L);
		String tableName = ((TableNode) node).getTableName();
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
		logger.info(logMsg);
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
			tapAdvanceFilter.limit(cdcPollingBatchSize);
			try {
				if (loopTime.get() == 1L || loopTime.get() % logLoopTime == 0) {
					logger.info("Query by advance filter\n - loop time: " + loopTime + "\n - table: " + tapTable.getId()
							+ "\n - filter: " + tapAdvanceFilter.getOperators()
							+ "\n - limit: " + tapAdvanceFilter.getLimit() + "\n - sort: " + tapAdvanceFilter.getSortOnList());
					obsLogger.info("Query by advance filter\n - loop time: " + loopTime + "\n - table: " + tapTable.getId()
							+ "\n - filter: " + tapAdvanceFilter.getOperators()
							+ "\n - limit: " + tapAdvanceFilter.getLimit() + "\n - sort: " + tapAdvanceFilter.getSortOnList());
				}
				PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
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
											tapInsertRecordEvent.addInfo(TAPEVENT_INFO_EVENT_ID_KEY, UUID.randomUUID().toString());
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
											tapdataEvent.setType(SyncProgress.Type.POLLING_CDC);
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
				}
				tablePollingCDCOffset.put(field, convertValue);
			}
			streamOffsetObj.put(tableName, tablePollingCDCOffset);
		} else {
			tablePollingCDCOffset = (Map<String, Object>) streamOffsetObj.get(tableName);
		}
		return tablePollingCDCOffset;
	}

	private void flushPollingCDCOffset(List<TapEvent> tapEvents) {
		if (CollectionUtils.isEmpty(tapEvents)) {
			return;
		}
		TapEvent lastEvent = tapEvents.get(tapEvents.size() - 1);
		flushPollingCDCOffset((TapInsertRecordEvent) lastEvent);
	}

	private void flushPollingCDCOffset(TapInsertRecordEvent tapEvent) {
		if (!isPollingCDC(getNode())) {
			return;
		}
		TableNode node = (TableNode) getNode();
		String tableName = node.getTableName();
		Map streamOffsetMap = (Map) syncProgress.getStreamOffsetObj();
		if (!streamOffsetMap.containsKey(tableName)) {
			streamOffsetMap.put(tableName, new HashMap<>());
		}
		Map tablePollingCDCOffset = (Map) streamOffsetMap.get(tableName);
		Map<String, Object> after = tapEvent.getAfter();
		for (String conditionField : conditionFields) {
			Object value = after.get(conditionField);
			tablePollingCDCOffset.put(conditionField, value);
		}
		TapCodecsFilterManager connecotrCodecsFilterManger = getConnectorNode().getCodecsFilterManager();
		toTapValue(tablePollingCDCOffset, tapEvent.getTableId(), connecotrCodecsFilterManger);
		fromTapValue(tablePollingCDCOffset, connecotrCodecsFilterManger);
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
		TableNode tableNode = (TableNode) dataProcessorContext.getNode();
		TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
		if (isTableFilter(tableNode)) {
			List<QueryOperator> conditions = tableNode.getConditions();
			if (CollectionUtils.isNotEmpty(conditions)) {
				DataMap match = new DataMap();
				List<QueryOperator> queryOperators = new ArrayList<>();
				for (QueryOperator queryOperator : conditions) {
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
			if (null == limit) {
				limit = readBatchSize;
			}
			tapAdvanceFilter.setLimit(limit);
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

}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.taskmilestones.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderController;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderService;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.sharecdc.ReaderType;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcReader;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcReaderExCode_13;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.flow.engine.V2.sharecdc.impl.ShareCdcFactory;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
	protected void doInit(@NotNull Context context) throws Exception {
		try {
			super.doInit(context);
			checkPollingCDCIfNeed();
		} catch (Throwable e) {
			//Notify error for task.
			throw errorHandle(e, "init failed");
		}
	}

	@Override
	public void startSourceRunner() {
		try {
			TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
			try {
				if (need2InitialSync(syncProgress)) {
					if (this.sourceRunnerFirstTime.get()) {
						doSnapshotWithControl(new ArrayList<>(tapTableMap.keySet()));
					}
				}

				if (!sourceRunnerFirstTime.get() && CollectionUtils.isNotEmpty(newTables)) {
					doSnapshot(newTables);
				}

				TaskDto taskDto = dataProcessorContext.getTaskDto();
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

	private void doSnapshotWithControl(List<String> tableList) throws Throwable {
		SnapshotOrderController controller = SnapshotOrderService.getInstance().getController(dataProcessorContext.getTaskDto().getId().toHexString());
		if (null != controller) {
			CommonUtils.AnyError runner = () -> doSnapshot(tableList);
			controller.runWithControl(getNode(), runner);
			controller.finish(getNode());
		}
	}

	@SneakyThrows
	protected void doSnapshot(List<String> tableList) {
		executeAspect(new SnapshotReadBeginAspect().dataProcessorContext(dataProcessorContext).tables(tableList));
		syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());

		// count the data size of the tables;
		doCount(tableList);

		BatchReadFunction batchReadFunction = getConnectorNode().getConnectorFunctions().getBatchReadFunction();
		QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = getConnectorNode().getConnectorFunctions().getQueryByAdvanceFilterFunction();
		ExecuteCommandFunction executeCommandFunction = getConnectorNode().getConnectorFunctions().getExecuteCommandFunction();

		if (batchReadFunction != null) {
			// MILESTONE-READ_SNAPSHOT-RUNNING
			if (sourceRunnerFirstTime.get()) {
				executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_START));
			}
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
							executeAspect(new SnapshotReadTableBeginAspect().dataProcessorContext(dataProcessorContext).tableName(tableName));
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
								obsLogger.info("Table " + tableName + " is detected that it has been removed, the snapshot read will be skipped");
								this.removeTables.remove(tableName);
								continue;
							}
							TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableName);
							Object tableOffset = ((Map<String, Object>) syncProgress.getBatchOffsetObj()).get(tapTable.getId());
							obsLogger.info("Starting batch read, table name: " + tapTable.getId() + ", offset: " + tableOffset);

							executeDataFuncAspect(
									BatchReadFuncAspect.class, () -> new BatchReadFuncAspect()
											.eventBatchSize(readBatchSize)
											.connectorContext(getConnectorNode().getConnectorContext())
											.offsetState(tableOffset)
											.dataProcessorContext(this.getDataProcessorContext())
											.start()
											.table(tapTable),
									batchReadFuncAspect -> PDKInvocationMonitor.invoke(
											getConnectorNode(),
											PDKMethod.SOURCE_BATCH_READ,
											createPdkMethodInvoker().runnable(() -> {
														BiConsumer<List<TapEvent>, Object> consumer = (events, offsetObject) -> {
															if (events != null && !events.isEmpty()) {
																events = events.stream().map(event -> {
																	if (null == event.getTime()) {
																		throw new NodeException("Invalid TapEvent, `TapEvent.time` should be NonNUll").context(getProcessorBaseContext()).event(event);
																	}
																	event.addInfo(TAPEVENT_INFO_EVENT_ID_KEY, UUID.randomUUID().toString());
																	return cdcDelayCalculation.filterAndCalcDelay(event, times -> AspectUtils.executeAspect(SourceCDCDelayAspect.class, () -> new SourceCDCDelayAspect().delay(times).dataProcessorContext(dataProcessorContext)));
																}).collect(Collectors.toList());

																if (batchReadFuncAspect != null)
																	AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_READ_COMPLETE).getReadCompleteConsumers(), events);

																if (obsLogger.isDebugEnabled()) {
																	obsLogger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(getConnectorNode()));
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
														Node<?> node = getNode();
														if (node instanceof TableNode) {
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
															} else if (tableNode.isEnableCustomCommand() && executeCommandFunction != null) {
																Map<String, Object> customCommand = tableNode.getCustomCommand();
																customCommand.put("batchSize", readBatchSize);
																executeCommandFunction.execute(getConnectorNode().getConnectorContext(), TapExecuteCommand.create()
																		.command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params")), executeResult -> {
																	if (executeResult.getError() != null) {
																		throw new NodeException("Execute error: " + executeResult.getError().getMessage(), executeResult.getError());
																	}
																	if (executeResult.getResult() == null) {
																		obsLogger.info("Execute result is null");
																		return;
																	}
																	List<Map<String, Object>> maps = (List<Map<String, Object>>) executeResult.getResult();
																	List<TapEvent> events = maps.stream().map(m -> TapSimplify.insertRecordEvent(m, tableName)).collect(Collectors.toList());
																	consumer.accept(events, null);
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
							executeAspect(new SnapshotReadTableEndAspect().dataProcessorContext(dataProcessorContext).tableName(tableName));
						} catch (Throwable throwable) {
							executeAspect(new SnapshotReadTableErrorAspect().dataProcessorContext(dataProcessorContext).tableName(tableName).error(throwable));
							Throwable throwableWrapper = throwable;
							if (!(throwableWrapper instanceof TapCodeException)) {
								throwableWrapper = new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, throwable);
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
					enqueue(new TapdataCompleteSnapshotEvent());
					//AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED));
				}
				AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED));
			}
		} else {
			throw new NodeException("PDK node does not support batch read: " + dataProcessorContext.getDatabaseType())
					.context(getProcessorBaseContext());
		}
		executeAspect(new SnapshotReadEndAspect().dataProcessorContext(dataProcessorContext));
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
						if (e instanceof TapCodeException) {
							throw e;
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
			Optional.of(cdcDelayCalculation.addHeartbeatTable(configWithTables.get(0).getTables())).map(joinHeartbeat -> executeAspect(SourceJoinHeartbeatAspect.class, () -> new SourceJoinHeartbeatAspect().dataProcessorContext(dataProcessorContext).joinHeartbeat(joinHeartbeat)));
			return connectorNode.getConnectorFunctions().getStreamReadMultiConnectionFunction();
		}).orElse(null);

		int batchSize = 1;
		String streamReadFunctionName = null;
		CommonUtils.AnyError anyError = null;
		List<String> tables = new ArrayList<>();
		if (null != streamReadMultiConnectionFunction) {
			streamReadFunctionName = streamReadMultiConnectionFunction.getClass().getSimpleName();
			anyError = () -> {
				streamReadMultiConnectionFunction.streamRead(getConnectorNode().getConnectorContext(), connectionConfigWithTables,
						syncProgress.getStreamOffsetObj(), batchSize, streamReadConsumer);
			};
		} else {
			RawDataCallbackFilterFunction rawDataCallbackFilterFunction = connectorNode.getConnectorFunctions().getRawDataCallbackFilterFunction();
			RawDataCallbackFilterFunctionV2 rawDataCallbackFilterFunctionV2 = connectorNode.getConnectorFunctions().getRawDataCallbackFilterFunctionV2();
//			if(rawDataCallbackFilterFunctionV2 != null) {
//				rawDataCallbackFilterFunction = null;
//			}
			StreamReadFunction streamReadFunction = connectorNode.getConnectorFunctions().getStreamReadFunction();
			if (null != streamReadFuncAspect && (null != rawDataCallbackFilterFunction || null != rawDataCallbackFilterFunctionV2)) {
				if (null != rawDataCallbackFilterFunctionV2) {
					streamReadFunctionName = rawDataCallbackFilterFunctionV2.getClass().getSimpleName();
				} else {
					streamReadFunctionName = rawDataCallbackFilterFunction.getClass().getSimpleName();
				}
				tables.addAll(tapTableMap.keySet());
				Optional.of(cdcDelayCalculation.addHeartbeatTable(tables)).map(joinHeartbeat -> executeAspect(SourceJoinHeartbeatAspect.class, () -> new SourceJoinHeartbeatAspect().dataProcessorContext(dataProcessorContext).joinHeartbeat(joinHeartbeat)));
				anyError = () -> {
					executeAspect(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_CALLBACK_RAW_DATA).streamReadConsumer(streamReadConsumer));
					while (isRunning()) {
						if (!streamReadFuncAspect.waitRawData()) {
							break;
						}
					}
					if (streamReadFuncAspect.getErrorDuringWait() != null) {
						throw streamReadFuncAspect.getErrorDuringWait();
					}
				};
			} else if (null != streamReadFunction) {
				streamReadFunctionName = streamReadFunction.getClass().getSimpleName();
				tables.addAll(tapTableMap.keySet());
				Optional.of(cdcDelayCalculation.addHeartbeatTable(tables)).map(joinHeartbeat -> executeAspect(SourceJoinHeartbeatAspect.class, () -> new SourceJoinHeartbeatAspect().dataProcessorContext(dataProcessorContext).joinHeartbeat(joinHeartbeat)));
				anyError = () -> {
					streamReadFunction.streamRead(getConnectorNode().getConnectorContext(), tables,
							syncProgress.getStreamOffsetObj(), batchSize, streamReadConsumer);
				};
			}
		}

		if (null != anyError) {
			obsLogger.info("Starting stream read, table list: " + tables + ", offset: " + syncProgress.getStreamOffsetObj());

			CommonUtils.AnyError finalAnyError = anyError;
			String finalStreamReadFunctionName = streamReadFunctionName;
			executeDataFuncAspect(StreamReadFuncAspect.class, () -> new StreamReadFuncAspect()
							.connectorContext(connectorNode.getConnectorContext())
							.dataProcessorContext(getDataProcessorContext())
							.streamReadFunction(finalStreamReadFunctionName)
							.tables(tables)
							.eventBatchSize(batchSize)
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
						event.addInfo("eventId", UUID.randomUUID().toString());
						return cdcDelayCalculation.filterAndCalcDelay(event, times -> AspectUtils.executeAspect(SourceCDCDelayAspect.class, () -> new SourceCDCDelayAspect().delay(times).dataProcessorContext(dataProcessorContext)));
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
			LogCollectorNode logCollectorNode = (LogCollectorNode) node;
			tapdataStartedCdcEvent.setType(SyncProgress.Type.LOG_COLLECTOR);
			tapdataStartedCdcEvent.addInfo(TapdataEvent.CONNECTION_ID_INFO_KEY, dataProcessorContext.getConnections().getId());
			tapdataStartedCdcEvent.addInfo(TapdataEvent.TABLE_NAMES_INFO_KEY, logCollectorNode.getTableNames());
		}
		enqueue(tapdataStartedCdcEvent);
	}

	private void doShareCdc() throws Exception {
		if (!isRunning()) {
			return;
		}
		Optional.of(cdcDelayCalculation.addHeartbeatTable(new ArrayList<>(dataProcessorContext.getTapTableMap().keySet())))
				.map(joinHeartbeat -> executeAspect(SourceJoinHeartbeatAspect.class, () -> new SourceJoinHeartbeatAspect().dataProcessorContext(dataProcessorContext).joinHeartbeat(joinHeartbeat)));
		ShareCdcTaskContext shareCdcTaskContext = new ShareCdcTaskPdkContext(getCdcStartTs(), processorBaseContext.getConfigurationCenter(),
				dataProcessorContext.getTaskDto(), dataProcessorContext.getNode(), dataProcessorContext.getSourceConn(), getConnectorNode());
		shareCdcTaskContext.setObsLogger(obsLogger);
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		List<String> tables = new ArrayList<>(tapTableMap.keySet());
		// Init share cdc reader, if unavailable, will throw ShareCdcUnsupportedException
		this.shareCdcReader = ShareCdcFactory.shareCdcReader(ReaderType.PDK_TASK_HAZELCAST, shareCdcTaskContext, syncProgress.getStreamOffsetObj());
		obsLogger.info("Starting incremental sync, read from share log storage...");
		this.syncProgressType = SyncProgress.Type.SHARE_CDC;
		// Start listen message entity from share storage log
		executeDataFuncAspect(StreamReadFuncAspect.class,
				() -> new StreamReadFuncAspect()
						.dataProcessorContext(getDataProcessorContext())
						.tables(tables)
						.eventBatchSize(1)
						.offsetState(syncProgress.getStreamOffsetObj())
						.start(),
				streamReadFuncAspect -> {
					this.streamReadFuncAspect = streamReadFuncAspect;
					this.shareCdcReader.listen(streamReadConsumer);
				});
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
				String tableName = tableNode.getTableName();
				TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableName);
				DataMap match = new DataMap();
				List<QueryOperator> queryOperators = new ArrayList<>();
				for (QueryOperator queryOperator : conditions) {
					TapField tapField = tapTable.getNameFieldMap().get(queryOperator.getKey());
					TapType tapType = tapField.getTapType();
					Object convertValue;
					if (queryOperator.getValue() != null) {
						convertValue = getConvertValue(tapType, queryOperator.getValue().toString());
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

}

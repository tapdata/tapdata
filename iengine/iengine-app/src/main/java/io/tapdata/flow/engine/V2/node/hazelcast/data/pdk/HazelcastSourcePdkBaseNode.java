package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import cn.hutool.core.util.ReUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.TapdataTaskErrorEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.cdcdelay.CdcDelayDisable;
import com.tapdata.tm.commons.cdcdelay.ICdcDelay;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.aspect.SourceCDCDelayAspect;
import io.tapdata.aspect.SourceDynamicTableAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TableCountFuncAspect;
import io.tapdata.aspect.TaskMilestoneFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.flow.engine.V2.ddl.DDLFilter;
import io.tapdata.flow.engine.V2.ddl.DDLSchemaHandler;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.monitor.impl.TableMonitor;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.sharecdc.ShareCDCOffset;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:59
 **/
public abstract class HazelcastSourcePdkBaseNode extends HazelcastPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	public static final long PERIOD_SECOND_HANDLE_TABLE_MONITOR_RESULT = 10L;
	public static final String TAPEVENT_INFO_EVENT_ID_KEY = "eventId";
	private static final int ASYNCLY_COUNT_SNAPSHOT_ROW_SIZE_TABLE_THRESHOLD = 100;
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkBaseNode.class);
	protected SyncProgress syncProgress;
	protected ExecutorService sourceRunner;
	protected ScheduledExecutorService tableMonitorResultHandler;
	protected SnapshotProgressManager snapshotProgressManager;

	/**
	 * This is added as an async control center because pdk and jet have two different thread model. pdk thread is
	 * blocked when reading data from data source while jet using async when passing the event to next node.
	 */
	protected LinkedBlockingQueue<TapdataEvent> eventQueue = new LinkedBlockingQueue<>(1024);
	protected StreamReadFuncAspect streamReadFuncAspect;
	private TapdataEvent pendingEvent;
	protected SourceMode sourceMode = SourceMode.NORMAL;
	protected Long initialFirstStartTime = System.currentTimeMillis();
	protected TransformerWsMessageDto transformerWsMessageDto;
	protected DDLFilter ddlFilter;
	protected ReentrantLock sourceRunnerLock;
	protected AtomicBoolean endSnapshotLoop;
	protected CopyOnWriteArrayList<String> newTables;
	protected CopyOnWriteArrayList<String> removeTables;
	protected AtomicBoolean sourceRunnerFirstTime;
	private DAGDataServiceImpl dagDataService;

	private Future<?> sourceRunnerFuture;
	// on cdc step if TableMap not exists heartbeat table, add heartbeat table to cdc whitelist and filter heartbeat records
	protected ICdcDelay cdcDelayCalculation;
	private final Object waitObj = new Object();
	protected DatabaseTypeEnum.DatabaseType databaseType;
	private boolean firstComplete = true;
	protected Map<String, Long> snapshotRowSizeMap;
	private ExecutorService snapshotRowSizeThreadPool;

	public HazelcastSourcePdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.cdcDelayCalculation = new CdcDelayDisable();
		// MILESTONE-INIT_CONNECTOR-RUNNING
		TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.RUNNING);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		try {
			createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
			connectorNodeInit(dataProcessorContext);
		} catch (Throwable e) {
			TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.ERROR, logger);
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			throw new NodeException(e).context(getProcessorBaseContext());
		}
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		syncProgress = initSyncProgress(taskDto.getAttrs());
		obsLogger.info("Found sync progress: " + syncProgress);
		if (!StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(),
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			initBatchAndStreamOffset(taskDto);
			obsLogger.info(String.format("Node %s[%s] batch offset: %s", getNode().getName(), getNode().getId(), JSONUtil.obj2Json(syncProgress.getBatchOffsetObj())));
			obsLogger.info(String.format("Node %s[%s] stream offset: %s", getNode().getName(), getNode().getId(), JSONUtil.obj2Json(syncProgress.getStreamOffsetObj())));
		}
		initDDLFilter();
		this.sourceRunnerLock = new ReentrantLock(true);
		this.endSnapshotLoop = new AtomicBoolean(false);
		this.transformerWsMessageDto = clientMongoOperator.findOne(new Query(),
				ConnectorConstant.TASK_COLLECTION + "/transformAllParam/" + processorBaseContext.getTaskDto().getId().toHexString(),
				TransformerWsMessageDto.class);
		this.sourceRunnerFirstTime = new AtomicBoolean(true);
		databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, dataProcessorContext.getConnections().getPdkHash());
		this.sourceRunner = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.SECONDS, new SynchronousQueue<>(),
				r -> {
					Thread thread = new Thread(r);
					thread.setName(String.format("Source-Runner-%s[%s]", getNode().getName(), getNode().getId()));
					return thread;
				});
		sourceRunnerFuture = this.sourceRunner.submit(this::startSourceRunner);
		initTableMonitor();
	}

	private void initDDLFilter() {
		Node<?> node = dataProcessorContext.getNode();
		if (node.isDataNode()) {
			Boolean enableDDL = ((DataParentNode<?>) node).getEnableDDL();
			List<String> disabledEvents = ((DataParentNode<?>) node).getDisabledEvents();
			this.ddlFilter = DDLFilter.create(enableDDL, disabledEvents).dynamicTableTest(this::needDynamicTable);
		}
	}

	private void initTableMonitor() throws Exception {
		Node<?> node = dataProcessorContext.getNode();
		if (node.isDataNode()) {
			if (needDynamicTable(null)) {
				this.newTables = new CopyOnWriteArrayList<>();
				this.removeTables = new CopyOnWriteArrayList<>();

				Predicate<String> dynamicTableFilter = t -> ReUtil.isMatch(((DatabaseNode) node).getTableExpression(), t);
				TableMonitor tableMonitor = new TableMonitor(dataProcessorContext.getTapTableMap(),
						associateId, dataProcessorContext.getTaskDto(), dataProcessorContext.getSourceConn(), dynamicTableFilter);
				this.monitorManager.startMonitor(tableMonitor);
				this.tableMonitorResultHandler = new ScheduledThreadPoolExecutor(1);
				this.tableMonitorResultHandler.scheduleAtFixedRate(this::handleTableMonitorResult, 0L, PERIOD_SECOND_HANDLE_TABLE_MONITOR_RESULT, TimeUnit.SECONDS);
				logger.info("Handle dynamic add/remove table thread started, interval: " + PERIOD_SECOND_HANDLE_TABLE_MONITOR_RESULT + " seconds");
			}
		}
	}

	private boolean needDynamicTable(String tableName) {
		Node<?> node = dataProcessorContext.getNode();
		if (node instanceof DatabaseNode) {
			String migrateTableSelectType = ((DatabaseNode) node).getMigrateTableSelectType();
			if (StringUtils.isBlank(migrateTableSelectType) || !"expression".equals(migrateTableSelectType)) {
				return false;
			}
			Boolean enableDynamicTable = ((DatabaseNode) node).getEnableDynamicTable();
			if (enableDynamicTable != null && !enableDynamicTable) {
				return false;
			}
			if (syncType.equals(SyncTypeEnum.INITIAL_SYNC)) {
				return false;
			}
			GetTableNamesFunction getTableNamesFunction = getConnectorNode().getConnectorFunctions().getGetTableNamesFunction();
			if (null == getTableNamesFunction) {
				return false;
			}
			if (StringUtils.isNotEmpty(tableName)) {
				String expression = ((DatabaseNode) node).getTableExpression();
				if (StringUtils.isEmpty(expression) || !ReUtil.isMatch(expression, tableName)) {
					return false;
				}
			}
		} else {
			return false;
		}
		return true;
	}

	//TODO Aplomb should NOT create stream offset at very beginning.
	private void initBatchAndStreamOffset(TaskDto taskDto) {
		if (syncProgress == null) {
			syncProgress = new SyncProgress();
			// null present current
			Long offsetStartTimeMs = null;
			switch (syncType) {
				case INITIAL_SYNC_CDC:
					if (isPollingCDC(getNode())) {
						syncProgress.setStreamOffsetObj(new HashMap<>());
					} else {
						initStreamOffsetFromTime(offsetStartTimeMs);
					}
					break;
				case INITIAL_SYNC:
					syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
					break;
				case CDC:
					if (isPollingCDC(getNode())) {
						syncProgress.setStreamOffsetObj(new HashMap<>());
					} else {
						List<TaskDto.SyncPoint> syncPoints = taskDto.getSyncPoints();
						TaskDto.SyncPoint syncPoint = null;
						if (null != syncPoints) {
							syncPoint = syncPoints.stream().filter(sp -> dataProcessorContext.getNode().getId().equals(sp.getNodeId())).findFirst().orElse(null);
						}
						String pointType = syncPoint == null ? "current" : syncPoint.getPointType();
						if (StringUtils.isBlank(pointType)) {
							throw new NodeException("Run cdc task failed, sync point type cannot be empty").context(getProcessorBaseContext());
						}
						switch (pointType) {
							case "localTZ":
							case "connTZ":
								// todo missing db timezone
								offsetStartTimeMs = syncPoint.getDateTime();
								break;
							case "current":
								break;
						}
						initStreamOffsetFromTime(offsetStartTimeMs);
					}
					break;
			}
			if (null == offsetStartTimeMs || offsetStartTimeMs.compareTo(0L) <= 0) {
				offsetStartTimeMs = syncProgress.getEventTime();
			} else {
				syncProgress.setEventTime(offsetStartTimeMs);
				syncProgress.setSourceTime(offsetStartTimeMs);
			}
			if (null != syncProgress.getStreamOffsetObj()) {
				TapdataEvent tapdataEvent = TapdataHeartbeatEvent.create(offsetStartTimeMs, syncProgress.getStreamOffsetObj());
				enqueue(tapdataEvent);
			}
		} else {
			String batchOffset = syncProgress.getBatchOffset();
			if (StringUtils.isNotBlank(batchOffset)) {
				syncProgress.setBatchOffsetObj(PdkUtil.decodeOffset(batchOffset, getConnectorNode()));
			} else {
				syncProgress.setBatchOffsetObj(new HashMap<>());
			}
			String streamOffset = syncProgress.getStreamOffset();
			if (null == syncProgress.getEventTime()) {
				syncProgress.setEventTime(syncProgress.getSourceTime());
			}
			SyncProgress.Type type = syncProgress.getType();
			switch (type) {
				case NORMAL:
				case LOG_COLLECTOR:
					if (StringUtils.isNotBlank(streamOffset)) {
						syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, getConnectorNode()));
					} else {
						initStreamOffsetFromTime(null);
					}
					break;
				case SHARE_CDC:
					if (((DataProcessorContext) processorBaseContext).getSourceConn().isShareCdcEnable()
							&& taskDto.getShareCdcEnable()) {
						// continue cdc from share log storage
						if (StringUtils.isNotBlank(streamOffset)) {
							Object decodeOffset = PdkUtil.decodeOffset(streamOffset, getConnectorNode());
							if (decodeOffset instanceof ShareCDCOffset) {
								syncProgress.setStreamOffsetObj(((ShareCDCOffset) decodeOffset).getSequenceMap());
							} else {
								syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, getConnectorNode()));
							}
						} else {
							initStreamOffsetFromTime(null);
						}
					} else {
						// switch share cdc to normal task
						if (StringUtils.isNotBlank(streamOffset)) {
							Object decodeOffset = PdkUtil.decodeOffset(streamOffset, getConnectorNode());
							if (decodeOffset instanceof ShareCDCOffset) {
								syncProgress.setStreamOffsetObj(((ShareCDCOffset) decodeOffset).getStreamOffset());
							} else {
								syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, getConnectorNode()));
							}
						} else {
							Long eventTime = syncProgress.getEventTime();
							Long sourceTime = syncProgress.getSourceTime();
							if (null == eventTime && null == sourceTime) {
								throw new NodeException("It was found that the task was switched from shared incremental to normal mode and cannot continue execution, reason: lost breakpoint timestamp."
										+ " Please try to reset and start the task.").context(getProcessorBaseContext());
							}
							initStreamOffsetFromTime(null == eventTime ? sourceTime : eventTime);
						}
					}
					break;
				case POLLING_CDC:
					if (StringUtils.isNotBlank(streamOffset)) {
						syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, getConnectorNode()));
					} else {
						syncProgress.setStreamOffsetObj(new HashMap<>());
					}
			}
		}
	}

	private void initStreamOffsetFromTime(Long offsetStartTimeMs) {
		AtomicReference<Object> timeToStreamOffsetResult = new AtomicReference<>();
		TimestampToStreamOffsetFunction timestampToStreamOffsetFunction = getConnectorNode().getConnectorFunctions().getTimestampToStreamOffsetFunction();
		if (null != timestampToStreamOffsetFunction) {
			PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TIMESTAMP_TO_STREAM_OFFSET, () -> {
				try {
					timeToStreamOffsetResult.set(timestampToStreamOffsetFunction.timestampToStreamOffset(getConnectorNode().getConnectorContext(), offsetStartTimeMs));
				} catch (Throwable e) {
					if (need2InitialSync(syncProgress)) {
						obsLogger.warn("Call timestamp to stream offset function failed, will stop task after snapshot, type: " + dataProcessorContext.getDatabaseType()
								+ ", errors: " + e.getClass().getSimpleName() + "  " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
						this.offsetFromTimeError = e;
					} else {
						throw new NodeException("Call timestamp to stream offset function failed, will stop task, type: " + dataProcessorContext.getDatabaseType()
								+ ", errors: " + e.getClass().getSimpleName() + "  " + e.getMessage() + "\n" + Log4jUtil.getStackString(e)).context(getProcessorBaseContext());
					}
				}
				syncProgress.setStreamOffsetObj(timeToStreamOffsetResult.get());
			}, TAG);
		} else {
			obsLogger.warn("Pdk connector does not support timestamp to stream offset function, will stop task after snapshot: " + dataProcessorContext.getDatabaseType());
		}
	}

	@Override
	final public boolean complete() {
		try {
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			if (firstComplete) {
				Log4jUtil.setThreadContext(taskDto);
				Thread.currentThread().setName(String.format("Source-Complete-%s[%s]", getNode().getName(), getNode().getId()));
				firstComplete = false;
			}
			TapdataEvent dataEvent = null;
			if (!isRunning()) {
				return null == error;
			}
			if (pendingEvent != null) {
				dataEvent = pendingEvent;
				pendingEvent = null;
			} else {
				try {
					dataEvent = eventQueue.poll(500, TimeUnit.MILLISECONDS);
				} catch (InterruptedException ignored) {
				}
				if (null != dataEvent) {
					// covert to tap value before enqueue the event. when the event is enqueued into the eventQueue,
					// the event is considered been output to the next node.
					TapCodecsFilterManager codecsFilterManager = getConnectorNode().getCodecsFilterManager();
					TapEvent tapEvent = dataEvent.getTapEvent();
					tapRecordToTapValue(tapEvent, codecsFilterManager);
				}
			}

			if (dataEvent != null) {
				if (!offer(dataEvent)) {
					pendingEvent = dataEvent;
					return false;
				}
			}

			if (sourceRunnerFuture != null && sourceRunnerFuture.isDone() && sourceRunnerFirstTime.get()
					&& null == pendingEvent && eventQueue.isEmpty()) {
				if (TaskDto.TYPE_INITIAL_SYNC.equals(taskDto.getType())) {
					Object residueSnapshot = getGlobalMap(getCompletedInitialKey());
					if (residueSnapshot instanceof Integer) {
						int residueSnapshotInt = (int) residueSnapshot;
						if (residueSnapshotInt <= 0) {
							this.running.set(false);
						}
					}
				} else {
					this.running.set(false);
				}
			}
			/*if (1 == 1) {
				Thread.sleep(5000L);
				throw new RuntimeException("test");
			}*/
		} catch (Exception e) {
			String errorMsg = String.format("Source sync failed: %s", e.getMessage());
			obsLogger.error(errorMsg, e);
//			throw new RuntimeException(errorMsg, e);
			errorHandle(e, errorMsg);
		} finally {
			ThreadContext.clearAll();
		}

		return false;
	}

	private void handleTableMonitorResult() {
		Thread.currentThread().setName("Handle-Table-Monitor-Result-" + this.associateId);
		try {
			Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
			// Handle dynamic table change
			Object tableMonitor = monitorManager.getMonitorByType(MonitorManager.MonitorType.TABLE_MONITOR);
			if (tableMonitor instanceof TableMonitor) {
				((TableMonitor) tableMonitor).consume(tableResult -> {
					try {
						List<String> addList = tableResult.getAddList();
						List<String> removeList = tableResult.getRemoveList();
						if (CollectionUtils.isNotEmpty(addList) || CollectionUtils.isNotEmpty(removeList)) {
							while (isRunning()) {
								try {
									if (sourceRunnerLock.tryLock(1L, TimeUnit.SECONDS)) {
										break;
									}
								} catch (InterruptedException e) {
									break;
								}
							}
							// Handle new table(s)
							if (CollectionUtils.isNotEmpty(addList)) {
								obsLogger.info("Found new table(s): " + addList);
								addList.forEach(a -> removeTables.remove(a));
								List<TapTable> addTapTables = new ArrayList<>();
								List<TapdataEvent> tapdataEvents = new ArrayList<>();
								// Load schema
								LoadSchemaRunner.pdkDiscoverSchema(getConnectorNode(), addList, addTapTables::add);
								obsLogger.info("Load new table's schema finished");
								if (CollectionUtils.isEmpty(addTapTables)) {
									String error = "Load new table schema failed, expect table count: " + addList.size() + ", actual: 0";
									errorHandle(new RuntimeException(error), error);
								}
								if (addList.size() != addTapTables.size()) {
									String error = "Load new table schema failed, expect table count: " + addList.size() + ", actual: " + addTapTables.size();
									errorHandle(new RuntimeException(error), error);
								}
								for (TapTable addTapTable : addTapTables) {
									if (!isRunning()) {
										break;
									}
									TapCreateTableEvent tapCreateTableEvent = new TapCreateTableEvent();
									tapCreateTableEvent.table(addTapTable);
									tapCreateTableEvent.setTableId(addTapTable.getId());
									TapdataEvent tapdataEvent = wrapTapdataEvent(tapCreateTableEvent, SyncStage.valueOf(syncProgress.getSyncStage()), null, false);
									if (null == tapdataEvent) {
										String error = "Wrap create table tapdata event failed: " + addTapTable;
										errorHandle(new RuntimeException(error), error);
										return;
									}
									tapdataEvents.add(tapdataEvent);
								}
								if (!isRunning()) {
									return;
								}
								tapdataEvents.forEach(this::enqueue);
								this.newTables.addAll(addList);
								AspectUtils.executeAspect(new SourceDynamicTableAspect()
										.dataProcessorContext(getDataProcessorContext())
										.type(SourceDynamicTableAspect.DYNAMIC_TABLE_TYPE_ADD)
										.tables(addList)
										.tapdataEvents(tapdataEvents));
								if (this.endSnapshotLoop.get()) {
									obsLogger.info("It is detected that the snapshot reading has ended, and the reading thread will be restarted");
									// Restart source runner
									if (null != sourceRunner) {
										this.sourceRunnerFirstTime.set(false);
										if (null != getConnectorNode()) {
											//Release webhook waiting thread before stop connectorNode.
											if (streamReadFuncAspect != null) {
												streamReadFuncAspect.noMoreWaitRawData();
												streamReadFuncAspect = null;
											}
											PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.STOP, () -> getConnectorNode().connectorStop(), TAG);
											PDKIntegration.releaseAssociateId(this.associateId);
											ConnectorNodeService.getInstance().removeConnectorNode(this.associateId);
											createPdkConnectorNode(dataProcessorContext, jetContext.hazelcastInstance());
											connectorNodeInit(dataProcessorContext);
										} else {
											String error = "Connector node is null";
											errorHandle(new RuntimeException(error), error);
											return;
										}
										this.sourceRunner.shutdownNow();
										this.sourceRunner = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
										sourceRunner.submit(this::startSourceRunner);
									} else {
										String error = "Source runner is null";
										errorHandle(new RuntimeException(error), error);
										return;
									}
								}
							}
							// Handle remove table(s)
							if (CollectionUtils.isNotEmpty(removeList)) {
								logger.info("Found remove table(s): " + removeList);
								removeList.forEach(r -> {
									if (!removeTables.contains(r)) {
										removeTables.add(r);
									}
								});
								List<TapdataEvent> tapdataEvents = new ArrayList<>();
								for (String tableName : removeList) {
									if (!isRunning()) {
										break;
									}
									TapDropTableEvent tapDropTableEvent = new TapDropTableEvent();
									tapDropTableEvent.setTableId(tableName);
									TapdataEvent tapdataEvent = wrapTapdataEvent(tapDropTableEvent, SyncStage.valueOf(syncProgress.getSyncStage()), null, false);
									tapdataEvents.add(tapdataEvent);
								}
								tapdataEvents.forEach(this::enqueue);
								AspectUtils.executeAspect(new SourceDynamicTableAspect()
										.dataProcessorContext(getDataProcessorContext())
										.type(SourceDynamicTableAspect.DYNAMIC_TABLE_TYPE_REMOVE)
										.tables(removeList)
										.tapdataEvents(tapdataEvents));
							}
						}
					} catch (Throwable throwable) {
						String error = "Handle table monitor result failed, result: " + tableResult + ", error: " + throwable.getMessage();
						throw new NodeException(error, throwable).context(getProcessorBaseContext());
					}
				});
			}
		} catch (Throwable throwable) {
			errorHandle(throwable, throwable.getMessage());
		} finally {
			try {
				sourceRunnerLock.unlock();
			} catch (Exception ignored) {
			}
		}
	}


	abstract void startSourceRunner();

	@NotNull
	public List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events) {
		return wrapTapdataEvent(events, SyncStage.INITIAL_SYNC, null);
	}

	@NotNull
	protected List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events, SyncStage syncStage, Object offsetObj) {
		List<TapdataEvent> tapdataEvents = new ArrayList<>(events.size() + 1);
		for (int i = 0; i < events.size(); i++) {
			TapEvent tapEvent = events.get(i);
			boolean isLast = i == (events.size() - 1);
			TapdataEvent tapdataEvent;
			tapdataEvent = wrapTapdataEvent(tapEvent, syncStage, offsetObj, isLast);
			if (null == tapdataEvent) {
				continue;
			}
			tapdataEvents.add(tapdataEvent);
		}
		return tapdataEvents;
	}

	protected TapdataEvent wrapTapdataEvent(TapEvent tapEvent, SyncStage syncStage, Object offsetObj, boolean isLast) {
		try {
			return wrapSingleTapdataEvent(tapEvent, syncStage, offsetObj, isLast);
		} catch (Throwable throwable) {
			throw new NodeException("Error wrap TapEvent, event: " + tapEvent + ", error: " + throwable
					.getMessage(), throwable)
					.context(getDataProcessorContext())
					.event(tapEvent);
		}
	}

	private TapdataEvent wrapSingleTapdataEvent(TapEvent tapEvent, SyncStage syncStage, Object offsetObj, boolean isLast) {
		tapEvent = cdcDelayCalculation.filterAndCalcDelay(tapEvent, times -> AspectUtils.executeAspect(SourceCDCDelayAspect.class, () -> new SourceCDCDelayAspect().delay(times).dataProcessorContext(dataProcessorContext)));

		TapdataEvent tapdataEvent = null;
		if (tapEvent instanceof TapRecordEvent) {
			TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
			switch (sourceMode) {
				case NORMAL:
					tapdataEvent = new TapdataEvent();
					break;
				case LOG_COLLECTOR:
					tapdataEvent = new TapdataShareLogEvent();
					break;
			}
			tapdataEvent.setTapEvent(tapRecordEvent);
			tapdataEvent.setSyncStage(syncStage);
			if (SyncStage.INITIAL_SYNC == syncStage) {
				if (isLast && !StringUtils.equalsAnyIgnoreCase(dataProcessorContext.getTaskDto().getSyncType(),
						TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
					Map<String, Object> batchOffsetObj = (Map<String, Object>) syncProgress.getBatchOffsetObj();
//					Map<String, Object> newMap = new HashMap<>();
//					try {
//						MapUtil.deepCloneMap(batchOffsetObj, newMap);
//					} catch (IllegalAccessException | InstantiationException e) {
//						throw new RuntimeException("Deep clone batch offset map failed: " + e.getMessage(), e);
//					}
					tapdataEvent.setBatchOffset(batchOffsetObj);
					tapdataEvent.setSourceTime(syncProgress.getSourceTime());
				}
			} else if (SyncStage.CDC == syncStage) {
				tapdataEvent.setStreamOffset(offsetObj);
				if (null == ((TapRecordEvent) tapEvent).getReferenceTime())
					throw new RuntimeException("Tap CDC event's reference time is null");
				tapdataEvent.setSourceTime(((TapRecordEvent) tapEvent).getReferenceTime());
			}
		} else if (tapEvent instanceof HeartbeatEvent) {
			tapdataEvent = TapdataHeartbeatEvent.create(((HeartbeatEvent) tapEvent).getReferenceTime(), offsetObj);
		} else if (tapEvent instanceof TapDDLEvent) {
			obsLogger.info("Source node received an ddl event: " + tapEvent);

			if (null != ddlFilter && !ddlFilter.test((TapDDLEvent) tapEvent)) {
				obsLogger.warn("DDL events are filtered\n - Event: " + tapEvent + "\n - Filter: " + JSON.toJSONString(ddlFilter));
				return null;
			}

			tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapEvent);
			tapdataEvent.setSyncStage(syncStage);
			tapdataEvent.setStreamOffset(offsetObj);
			tapdataEvent.setSourceTime(((TapDDLEvent) tapEvent).getReferenceTime());
			String tableId = ((TapDDLEvent) tapEvent).getTableId();
			TapTable tapTable;
			// Modify schema by ddl event
			if (tapEvent instanceof TapCreateTableEvent) {
				tapTable = ((TapCreateTableEvent) tapEvent).getTable();
			} else {
				try {
					tapTable = processorBaseContext.getTapTableMap().get(tableId);
					InstanceFactory.bean(DDLSchemaHandler.class).updateSchemaByDDLEvent((TapDDLEvent) tapEvent, tapTable);
					TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
					DefaultExpressionMatchingMap dataTypesMap = getConnectorNode().getConnectorContext().getSpecification().getDataTypesMap();
					tableFieldTypesGenerator.autoFill(tapTable.getNameFieldMap(), dataTypesMap);
				} catch (Exception e) {
					throw errorHandle(e, "Modify schema by ddl failed, ddl type: " + tapEvent.getClass() + ", error: " + e.getMessage());
				}
			}

			// Refresh task config by ddl event
			DAG dag = processorBaseContext.getTaskDto().getDag();
			try {
				// Update DAG config
				dag.filedDdlEvent(processorBaseContext.getNode().getId(), (TapDDLEvent) tapEvent);
				DAG cloneDag = dag.clone();
				// Put new DAG into info map
				tapEvent.addInfo(NEW_DAG_INFO_KEY, cloneDag);
			} catch (Exception e) {
				throw errorHandle(e, "Update DAG by TapDDLEvent failed, error: " + e.getMessage());
			}
			// Refresh task schema by ddl event
			try {
				List<MetadataInstancesDto> insertMetadata = new CopyOnWriteArrayList<>();
				Map<String, MetadataInstancesDto> updateMetadata = new ConcurrentHashMap<>();
				List<String> removeMetadata = new CopyOnWriteArrayList<>();
				if (null == transformerWsMessageDto) {
					transformerWsMessageDto = clientMongoOperator.findOne(new Query(),
							ConnectorConstant.TASK_COLLECTION + "/transformAllParam/" + processorBaseContext.getTaskDto().getId().toHexString(),
							TransformerWsMessageDto.class);
				}
				List<MetadataInstancesDto> metadataInstancesDtoList = transformerWsMessageDto.getMetadataInstancesDtoList();
				Map<String, String> qualifiedNameIdMap = metadataInstancesDtoList.stream()
						.collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, m -> m.getId().toHexString()));
				tapEvent.addInfo(QUALIFIED_NAME_ID_MAP_INFO_KEY, qualifiedNameIdMap);
				if (null == dagDataService) {
					dagDataService = new DAGDataServiceImpl(transformerWsMessageDto);
				}
				String qualifiedName;
				Map<String, List<Message>> errorMessage;
				if (tapEvent instanceof TapCreateTableEvent) {
					qualifiedName = dagDataService.createNewTable(dataProcessorContext.getSourceConn().getId(), tapTable, processorBaseContext.getTaskDto().getId().toHexString());
					obsLogger.info("Create new table in memory, qualified name: " + qualifiedName);
					dataProcessorContext.getTapTableMap().putNew(tapTable.getId(), tapTable, qualifiedName);
					errorMessage = dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions());
					TaskDto taskDto = dagDataService.getTaskById(processorBaseContext.getTaskDto().getId().toHexString());
					taskDto.setDag(dag);
					MetadataInstancesDto metadata = dagDataService.getMetadata(qualifiedName);
					if (null == metadata.getId()) {
						metadata.setId(new ObjectId());
					}
					insertMetadata.add(metadata);
					obsLogger.info("Create new table schema transform finished: " + tapTable);
				} else if (tapEvent instanceof TapDropTableEvent) {
					qualifiedName = dataProcessorContext.getTapTableMap().getQualifiedName(((TapDropTableEvent) tapEvent).getTableId());
					obsLogger.info("Drop table in memory qualified name: " + qualifiedName);
					dagDataService.dropTable(qualifiedName);
					errorMessage = dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions());
					removeMetadata.add(qualifiedName);
					obsLogger.info("Drop table schema transform finished");
				} else {
					qualifiedName = dataProcessorContext.getTapTableMap().getQualifiedName(tableId);
					obsLogger.info("Alter table in memory, qualified name: " + qualifiedName);
					dagDataService.coverMetaDataByTapTable(qualifiedName, tapTable);
					errorMessage = dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions());
					MetadataInstancesDto metadata = dagDataService.getMetadata(qualifiedName);
					if (metadata.getId() == null) {
						metadata.setId(metadata.getOldId());
					}
					updateMetadata.put(metadata.getId().toHexString(), metadata);
					obsLogger.info("Alter table schema transform finished");
				}
				tapEvent.addInfo(INSERT_METADATA_INFO_KEY, insertMetadata);
				tapEvent.addInfo(UPDATE_METADATA_INFO_KEY, updateMetadata);
				tapEvent.addInfo(REMOVE_METADATA_INFO_KEY, removeMetadata);
				tapEvent.addInfo(DAG_DATA_SERVICE_INFO_KEY, dagDataService);
				tapEvent.addInfo(TRANSFORM_SCHEMA_ERROR_MESSAGE_INFO_KEY, errorMessage);
			} catch (Throwable e) {
				throw new RuntimeException("Transform schema by TapDDLEvent " + tapEvent + " failed, error: " + e.getMessage(), e);
			}
		}
		if (null == tapdataEvent) {
			throw new RuntimeException("Found event type does not support: " + tapEvent.getClass().getSimpleName());
		}
		return tapdataEvent;
	}

	public void enqueue(TapdataEvent tapdataEvent) {
		try {
			if (tapdataEvent.getTapEvent() instanceof TapRecordEvent) {
				String tableId = ((TapRecordEvent) tapdataEvent.getTapEvent()).getTableId();
				if (removeTables != null && removeTables.contains(tableId)) {
					return;
				}
			}

			while (isRunning()) {
				if (eventQueue.offer(tapdataEvent, 3, TimeUnit.SECONDS)) {
					break;
				}
			}
		} catch (InterruptedException ignore) {
			logger.warn("TapdataEvent enqueue thread interrupted");
		} catch (Throwable throwable) {
			throw new NodeException(throwable).context(getDataProcessorContext()).event(tapdataEvent.getTapEvent());
		}
	}

	@Override
	protected boolean need2CDC() {
		if (null != offsetFromTimeError) {
			enqueue(new TapdataTaskErrorEvent(offsetFromTimeError));
			try {
				synchronized (this.waitObj) {
					waitObj.wait();
				}
			} catch (InterruptedException ignored) {
			}
			return false;
		}
		return super.need2CDC();
	}

	@SneakyThrows
	protected void doCount(List<String> tableList) {
		BatchCountFunction batchCountFunction = getConnectorNode().getConnectorFunctions().getBatchCountFunction();
		if (null == batchCountFunction) {
			setDefaultRowSizeMap();
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
							obsLogger.warn("Query snapshot row size failed: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
						} else {
							obsLogger.info("Query snapshot row size completed: " + node.get().getName() + "(" + node.get().getId() + ")");
						}
						ExecutorUtil.shutdown(this.snapshotRowSizeThreadPool, 10L, TimeUnit.SECONDS);
					});
		} else {
			doCountSynchronously(batchCountFunction, tableList);
		}
	}

	private void setDefaultRowSizeMap() {
		for (String tableName : dataProcessorContext.getTapTableMap().keySet()) {
			if (null == snapshotRowSizeMap) {
				snapshotRowSizeMap = new HashMap<>();
			}
			snapshotRowSizeMap.putIfAbsent(tableName, 0L);
		}
	}

	@SneakyThrows
	protected void doCountSynchronously(BatchCountFunction batchCountFunction, List<String> tableList) {
		if (null == batchCountFunction) {
			setDefaultRowSizeMap();
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

	protected boolean isPollingCDC(Node<?> node) {
		return !SyncTypeEnum.INITIAL_SYNC.equals(syncType) && node instanceof TableNode && "polling".equals(((TableNode) node).getCdcMode());
	}

	@Override
	public void doClose() throws Exception {
		try {
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(waitObj).ifPresent(w -> {
				synchronized (this.waitObj) {
					this.waitObj.notify();
				}
			}), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(tableMonitorResultHandler).ifPresent(ExecutorService::shutdownNow), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(sourceRunner).ifPresent(ExecutorService::shutdownNow), TAG);
		} finally {
			super.doClose();
		}
	}

	public LinkedBlockingQueue<TapdataEvent> getEventQueue() {
		return eventQueue;
	}

	public SnapshotProgressManager getSnapshotProgressManager() {
		return snapshotProgressManager;
	}

	public enum SourceMode {
		NORMAL,
		LOG_COLLECTOR,
	}

	public SyncProgress getSyncProgress() {
		return syncProgress;
	}
}

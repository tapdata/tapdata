package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import cn.hutool.core.util.ReUtil;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.cdcdelay.CdcDelay;
import com.tapdata.tm.commons.cdcdelay.CdcDelayDisable;
import com.tapdata.tm.commons.cdcdelay.ICdcDelay;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.DDLConfiguration;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.UnionProcessorNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import com.tapdata.tm.commons.util.NoPrimaryKeyTableSelectType;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.aspect.SourceCDCDelayAspect;
import io.tapdata.aspect.SourceDynamicTableAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TableCountFuncAspect;
import io.tapdata.aspect.supervisor.DataNodeThreadGroupAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.ddl.DDLFilter;
import io.tapdata.flow.engine.V2.ddl.DDLSchemaHandler;
import io.tapdata.flow.engine.V2.filter.FilterUtil;
import io.tapdata.flow.engine.V2.filter.TargetTableDataEventFilter;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.monitor.impl.TableMonitor;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryContext;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryService;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.impl.DynamicAdjustMemoryImpl;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.sharecdc.ShareCDCOffset;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:59
 **/
public abstract class HazelcastSourcePdkBaseNode extends HazelcastPdkBaseNode {
	private static final String TAG = HazelcastSourcePdkBaseNode.class.getSimpleName();
	public static final long PERIOD_SECOND_HANDLE_TABLE_MONITOR_RESULT = 10L;
	private static final int ASYNCLY_COUNT_SNAPSHOT_ROW_SIZE_TABLE_THRESHOLD = 100;
	public static final long DEFAULT_RAM_THRESHOLD_BYTE = 3 * 1024L;
	public static final double DEFAULT_SAMPLE_RATE = 0.2D;
	public static final int MIN_QUEUE_SIZE = 10;
	public static final int SOURCE_QUEUE_FACTOR = 2;
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkBaseNode.class);
	protected SyncProgress syncProgress;
	protected ThreadPoolExecutorEx sourceRunner;
	protected ScheduledExecutorService tableMonitorResultHandler;
	protected SnapshotProgressManager snapshotProgressManager;
	protected int sourceQueueCapacity;
	protected int originalSourceQueueCapacity;
	private TargetTableDataEventFilter tapEventFilter;

	/**
	 * This is added as an async control center because pdk and jet have two different thread model. pdk thread is
	 * blocked when reading data from data source while jet using async when passing the event to next node.
	 */
	protected LinkedBlockingQueue<TapdataEvent> eventQueue;
	private final AtomicReference<Object> lastStreamOffset = new AtomicReference<>();
	protected StreamReadFuncAspect streamReadFuncAspect;
	protected TapdataEvent pendingEvent;
	protected SourceMode sourceMode = SourceMode.NORMAL;
	protected Long initialFirstStartTime = System.currentTimeMillis();
	protected TransformerWsMessageDto transformerWsMessageDto;
	protected DDLFilter ddlFilter;
	protected ReentrantLock sourceRunnerLock;
	protected AtomicBoolean endSnapshotLoop;
	protected CopyOnWriteArrayList<String> newTables;
	protected CopyOnWriteArrayList<String> removeTables;
	protected AtomicBoolean sourceRunnerFirstTime;
	protected Future<?> sourceRunnerFuture;
	// on cdc step if TableMap not exists heartbeat table, add heartbeat table to cdc whitelist and filter heartbeat records
	protected ICdcDelay cdcDelayCalculation;
	private final Object waitObj = new Object();
	protected DatabaseTypeEnum.DatabaseType databaseType;
	protected boolean firstComplete = true;
	protected Map<String, Long> snapshotRowSizeMap;
	private ExecutorService snapshotRowSizeThreadPool;
	private ConnectorOnTaskThreadGroup connectorOnTaskThreadGroup;
	protected DynamicAdjustMemoryService dynamicAdjustMemoryService;
	private ConcurrentHashMap<String, Connections> connectionMap = new ConcurrentHashMap<>();

	public HazelcastSourcePdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.tapEventFilter = TargetTableDataEventFilter.create();
	}

	private boolean needCdcDelay() {
		if (Boolean.TRUE.equals(dataProcessorContext.getConnections().getHeartbeatEnable())) {
			return Optional.ofNullable(dataProcessorContext.getTapTableMap()).map(tapTableMap -> {
				try {
					TapTable tapTable = tapTableMap.get(ConnHeartbeatUtils.TABLE_NAME);
					if (null != tapTable && StringUtils.isNotBlank(tapTable.getId()) && MapUtils.isNotEmpty(tapTable.getNameFieldMap())) {
						return true;
					}
					logger.warn("Check cdcDelay failed, schema: {}", tapTable);
					return false;
				} catch (Exception e) {
					logger.warn("Check cdcDelay failed: {}", e.getMessage());
					return false;
				}
			}).orElse(false);
		}
		return false;
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		if (needCdcDelay()) {
			this.cdcDelayCalculation = new CdcDelay();
		} else {
			this.cdcDelayCalculation = new CdcDelayDisable();
		}
		if (connectorOnTaskThreadGroup == null)
			connectorOnTaskThreadGroup = new ConnectorOnTaskThreadGroup(dataProcessorContext);
		this.sourceRunner = AsyncUtils.createThreadPoolExecutor(String.format("Source-Runner-%s[%s]", getNode().getName(), getNode().getId()), 2, connectorOnTaskThreadGroup, TAG);
		this.sourceRunner.submitSync(() -> {
			super.doInit(context);
			try {
				createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
				AspectUtils.executeAspect(DataNodeThreadGroupAspect.class, () ->
						new DataNodeThreadGroupAspect(this.getNode(), associateId, Thread.currentThread().getThreadGroup())
								.dataProcessorContext(dataProcessorContext));
				connectorNodeInit(dataProcessorContext);
			} catch (Throwable e) {
				throw new NodeException(e).context(getProcessorBaseContext());
			}
			initSourceReadBatchSize();
			initSourceEventQueue();
			initSyncProgress();
			initDDLFilter();
			initTapEventFilter();
			initTableMonitor();
			initDynamicAdjustMemory();
			initSourceRunnerOnce();
			initAndStartSourceRunner();
		});
	}

	private void initTapEventFilter() {
		if (null == processorBaseContext) {
			throw new CoreException("ProcessorBaseContext can not be empty");
		}
		TaskDto taskDto = processorBaseContext.getTaskDto();
		if (null == taskDto) {
			throw new CoreException("TaskDto can not be empty");
		}
		if (Boolean.TRUE.equals(processorBaseContext.getTaskDto().getNeedFilterEventData())) {
			TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
			if (null == tapTableMap) {
				throw new CoreException("TapTableMap can not be empty");
			}
			tapEventFilter.addHandler(event -> {
				TapEvent e = event.getTapEvent();
				try {
					if (e instanceof TapRecordEvent) {
						String tableId = ShareCdcUtil.getTapRecordEventTableName((TapRecordEvent) e);
						TapTable tapTable = null;
						try {
							tapTable = tapTableMap.get(tableId);
						} catch (Exception exception) {
							obsLogger.warn("Can not get table from TapTableMap, table name is: {}, error message: {}", tableId, exception.getMessage());
							return event;
						}
						FilterUtil.filterEventData(tapTable, e);
					}
				} catch (Exception exception) {
					throw new CoreException("Fail to automatically block new fields, message: {}", exception.getMessage(), exception.getCause());
				}
				return event;
			});
			obsLogger.info("Before the event is output to the target from source, it will automatically block field changes");
		}
	}

	@Override
	protected void doInitWithDisableNode(@NotNull Context context) throws TapCodeException {
		if (connectorOnTaskThreadGroup == null)
			connectorOnTaskThreadGroup = new ConnectorOnTaskThreadGroup(dataProcessorContext);
		this.sourceRunner = AsyncUtils.createThreadPoolExecutor(String.format("Source-Runner-%s[%s]", getNode().getName(), getNode().getId()), 2, connectorOnTaskThreadGroup, TAG);
		this.sourceRunner.submitSync(() -> {
			super.doInitWithDisableNode(context);
			try {
				createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
				AspectUtils.executeAspect(DataNodeThreadGroupAspect.class, () ->
						new DataNodeThreadGroupAspect(this.getNode(), associateId, Thread.currentThread().getThreadGroup())
								.dataProcessorContext(dataProcessorContext));
				connectorNodeInit(dataProcessorContext);
			} catch (Throwable e) {
				throw new NodeException(e).context(getProcessorBaseContext());
			}
			initSourceReadBatchSize();
			initSourceEventQueue();
			initSyncProgress();
		});
	}

	private void initSyncProgress() throws JsonProcessingException {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		Node node = getNode();
		this.syncProgress = foundSyncProgress(taskDto.getAttrs());
		if (null == this.syncProgress) {
			obsLogger.info("On the first run, the breakpoint will be initialized", node.getName());
		} else {
			obsLogger.info("Found exists breakpoint, will decode batch/stream offset", node.getName());
		}
		if (!StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(),
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			initBatchAndStreamOffset(taskDto);
			String offsetLog = "";
			if (null != syncProgress.getBatchOffsetObj()) {
				offsetLog += String.format("batch offset found: %s,", JSONUtil.obj2Json(syncProgress.getBatchOffsetObj()));
			} else {
				offsetLog += "batch offset not found, ";
			}
			if (null != syncProgress.getStreamOffsetObj()) {
				offsetLog += String.format("stream offset found: %s", JSONUtil.obj2Json(syncProgress.getStreamOffsetObj()));
			} else {
				offsetLog += "stream offset not found.";
			}
			obsLogger.info(offsetLog);
		}
	}

	private void initSourceRunnerOnce() {
		this.sourceRunnerLock = new ReentrantLock(true);
		this.endSnapshotLoop = new AtomicBoolean(false);
		this.transformerWsMessageDto = clientMongoOperator.findOne(new Query(),
				ConnectorConstant.TASK_COLLECTION + "/transformAllParam/" + processorBaseContext.getTaskDto().getId().toHexString(),
				TransformerWsMessageDto.class);
		this.sourceRunnerFirstTime = new AtomicBoolean(true);
		this.databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, dataProcessorContext.getConnections().getPdkHash());
	}

	private void initAndStartSourceRunner() {
		this.lastStreamOffset.set(syncProgress.getStreamOffset());
		this.sourceRunnerFuture = this.sourceRunner.submit(this::startSourceRunner);
	}

	private void initSourceEventQueue() {
		this.sourceQueueCapacity = readBatchSize * SOURCE_QUEUE_FACTOR;
		this.originalSourceQueueCapacity = sourceQueueCapacity;
		this.eventQueue = new LinkedBlockingQueue<>(sourceQueueCapacity);
		obsLogger.info("Source node \"{}\" event queue capacity: {}", getNode().getName(), sourceQueueCapacity);
	}

	private void initSourceReadBatchSize() {
		this.readBatchSize = DEFAULT_READ_BATCH_SIZE;
		this.increaseReadSize = DEFAULT_INCREASE_BATCH_SIZE;
		if (getNode() instanceof DataParentNode) {
			this.readBatchSize = Optional.ofNullable(((DataParentNode<?>) dataProcessorContext.getNode()).getReadBatchSize()).orElse(DEFAULT_READ_BATCH_SIZE);
			this.increaseReadSize = Optional.ofNullable(((DataParentNode<?>) dataProcessorContext.getNode()).getIncreaseReadSize()).orElse(DEFAULT_READ_BATCH_SIZE);
		}
		obsLogger.info("Source node \"{}\" read batch size: {}", getNode().getName(), readBatchSize);
	}

	protected void initDDLFilter() {
		Node<?> node = dataProcessorContext.getNode();
		if (node.isDataNode()) {
			List<String> disabledEvents = ((DataParentNode<?>) node).getDisabledEvents();
			DDLConfiguration ddlConfiguration = ((DataParentNode<?>) node).getDdlConfiguration();
			String ignoreDDLRules = ((DataParentNode<?>) node).getIgnoredDDLRules();
			this.ddlFilter = DDLFilter.create(disabledEvents, ddlConfiguration, ignoreDDLRules, obsLogger).dynamicTableTest(this::needDynamicTable);
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

	protected void initBatchAndStreamOffset(TaskDto taskDto) {
		if (syncProgress == null) {
			initBatchAndStreamOffsetFirstTime(taskDto);
		} else {
			readBatchAndStreamOffset(taskDto);
		}
	}

	protected void readBatchAndStreamOffset(TaskDto taskDto) {
		readBatchOffset();
		readStreamOffset(taskDto);
	}

	protected void readStreamOffset(TaskDto taskDto) {
		String streamOffset = syncProgress.getStreamOffset();
		if (null == syncProgress.getEventTime()) {
			syncProgress.setEventTime(syncProgress.getSourceTime());
		}
		SyncProgress.Type type = syncProgress.getType();
		switch (type) {
			case NORMAL:
			case LOG_COLLECTOR:
				readNormalAndLogCollectorTaskStreamOffset(streamOffset);
				break;
			case SHARE_CDC:
				readShareCDCStreamOffset(taskDto, streamOffset);
				break;
			case POLLING_CDC:
				readPollingCDCStreamOffset(streamOffset);
				break;
			default:
				throw new TapCodeException(TaskProcessorExCode_11.READ_STREAM_OFFSET_UNKNOWN_TASK_TYPE, "Unknown task type: " + type);
		}
	}

	protected void readPollingCDCStreamOffset(String streamOffset) {
		if (StringUtils.isNotBlank(streamOffset)) {
			streamOffset = uncompressStreamOffsetIfNeed(streamOffset);
			syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, getConnectorNode()));
		} else {
			syncProgress.setStreamOffsetObj(new HashMap<>());
		}
	}

	protected void readShareCDCStreamOffset(TaskDto taskDto, String streamOffset) {
		if (dataProcessorContext.getSourceConn().isShareCdcEnable()
				&& Boolean.TRUE.equals(taskDto.getShareCdcEnable())) {
			readShareCDCStreamOffsetContinueShareCDC(streamOffset);
		} else {
			readShareCDCStreamOffsetSwitchNormalTask(streamOffset);
		}
	}

	protected void readShareCDCStreamOffsetSwitchNormalTask(String streamOffset) {
		// switch share cdc to normal task
		if (StringUtils.isNotBlank(streamOffset)) {
			String streamOffsetStr = syncProgress.getStreamOffset();
			streamOffsetStr = uncompressStreamOffsetIfNeed(streamOffsetStr);
			Object decodeOffset = PdkUtil.decodeOffset(streamOffsetStr, getConnectorNode());
			if (decodeOffset instanceof ShareCDCOffset) {
				syncProgress.setStreamOffsetObj(((ShareCDCOffset) decodeOffset).getStreamOffset());
			} else {
				syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, getConnectorNode()));
			}
		} else {
			Long eventTime = syncProgress.getEventTime();
			Long sourceTime = syncProgress.getSourceTime();
			if (null == eventTime && null == sourceTime) {
				throw new TapCodeException(TaskProcessorExCode_11.SHARE_CDC_SWITCH_TO_NORMAL_TASK_FAILED,
						"It was found that the task was switched from shared incremental to normal mode and cannot continue execution, " +
								"reason: lost breakpoint timestamp, please try to reset and start the task.");
			}
			initStreamOffsetFromTime(null == eventTime ? sourceTime : eventTime);
		}
	}

	protected void readShareCDCStreamOffsetContinueShareCDC(String streamOffset) {
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
	}

	protected void readNormalAndLogCollectorTaskStreamOffset(String streamOffset) {
		if (StringUtils.isNotBlank(streamOffset)) {
			streamOffset = uncompressStreamOffsetIfNeed(streamOffset);
			syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, getConnectorNode()));
		} else {
			if (syncType == SyncTypeEnum.INITIAL_SYNC_CDC || syncType == SyncTypeEnum.CDC) {
				initStreamOffsetFromTime(null);
			}
		}
	}

	protected void readBatchOffset() {
		if (null == syncProgress) {
			return;
		}
		String batchOffset = syncProgress.getBatchOffset();
		if (StringUtils.isNotBlank(batchOffset)) {
			syncProgress.setBatchOffsetObj(PdkUtil.decodeOffset(batchOffset, getConnectorNode()));
		} else {
			syncProgress.setBatchOffsetObj(new HashMap<>());
		}
	}

	protected void initBatchAndStreamOffsetFirstTime(TaskDto taskDto) {
		syncProgress = new SyncProgress();
		// null present current
		Long offsetStartTimeMs = null;
		switch (syncType) {
			case INITIAL_SYNC_CDC:
				initStreamOffsetInitialAndCDC(offsetStartTimeMs);
				break;
			case INITIAL_SYNC:
				initStreamOffsetInitial();
				break;
			case CDC:
				offsetStartTimeMs = initStreamOffsetCDC(taskDto, offsetStartTimeMs);
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
			if (!SyncTypeEnum.CDC.equals(syncType)) {
				tapdataEvent.setSyncStage(SyncStage.INITIAL_SYNC);
			}
			enqueue(tapdataEvent);
		}
	}

	protected Long initStreamOffsetCDC(TaskDto taskDto, Long offsetStartTimeMs) {
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
				throw new TapCodeException(TaskProcessorExCode_11.INIT_STREAM_OFFSET_SYNC_POINT_TYPE_IS_EMPTY);
			}
			switch (pointType) {
				case "localTZ":
				case "connTZ":
					offsetStartTimeMs = syncPoint.getDateTime();
					break;
				case "current":
					break;
				default:
					throw new TapCodeException(TaskProcessorExCode_11.INIT_STREAM_OFFSET_UNKNOWN_POINT_TYPE, "Unknown start point type: " + pointType);

			}
			initStreamOffsetFromTime(offsetStartTimeMs);
		}
		return offsetStartTimeMs;
	}

	protected void initStreamOffsetInitial() {
		syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
	}

	protected void initStreamOffsetInitialAndCDC(Long offsetStartTimeMs) {
		if (isPollingCDC(getNode())) {
			syncProgress.setStreamOffsetObj(new HashMap<>());
		} else {
			initStreamOffsetFromTime(offsetStartTimeMs);
		}
	}

	@Nullable
	private String uncompressStreamOffsetIfNeed(String streamOffsetStr) {
		if (StringUtils.startsWith(syncProgress.getStreamOffset(), STREAM_OFFSET_COMPRESS_PREFIX)) {
			try {
				streamOffsetStr = StringCompression.uncompress(StringUtils.removeStart(streamOffsetStr, STREAM_OFFSET_COMPRESS_PREFIX));
			} catch (IOException e) {
				throw new RuntimeException("Uncompress stream offset failed: " + streamOffsetStr, e);
			}
		}
		return streamOffsetStr;
	}

	protected void initStreamOffsetFromTime(Long offsetStartTimeMs) {
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
				Thread.currentThread().setName(String.format("Source-Complete-%s[%s]", getNode().getName(), getNode().getId()));
				firstComplete = false;
			}
			TapdataEvent dataEvent = null;
			if (!isRunning()) {
				return true;
			}
			if (getNode().disabledNode()) {
				return false;
			}
			if (pendingEvent != null) {
				dataEvent = pendingEvent;
				pendingEvent = null;
			} else {
				if (null != eventQueue) {
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
			}

			if (dataEvent != null) {
				if (!offer(dataEvent)) {
					pendingEvent = dataEvent;
					return false;
				}
			}

			if (sourceRunnerFuture != null && sourceRunnerFuture.isDone() && sourceRunnerFirstTime.get()
					&& null == pendingEvent && eventQueue.isEmpty()) {
				Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE.getTaskGlobalVariable(taskDto.getId().toHexString());
				Object obj = taskGlobalVariable.get(TaskGlobalVariable.SOURCE_INITIAL_COUNTER_KEY);
				if (obj instanceof AtomicInteger) {
					if (((AtomicInteger) obj).get() <= 0) {
						this.running.set(false);
					}
				} else {
					this.running.set(false);
				}
			}

		} catch (Exception e) {
			String errorMsg = String.format("Source sync failed: %s", e.getMessage());

			errorHandle(e, errorMsg);
		} finally {
			ThreadContext.clearAll();
		}

		return false;
	}

	protected void handleTableMonitorResult() {
		Thread.currentThread().setName("Handle-Table-Monitor-Result-" + this.associateId);
		try {
			// Handle dynamic table change
			Object tableMonitor = monitorManager.getMonitorByType(MonitorManager.MonitorType.TABLE_MONITOR);
			if (tableMonitor instanceof TableMonitor) {
				((TableMonitor) tableMonitor).consume(tableResult -> {
					try {
						List<String> addList = tableResult.getAddList();
						List<String> removeList = tableResult.getRemoveList();
						List<String> loadedTableNames;
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
								addList.forEach(tableName -> removeTables.remove(tableName));
								if (handleNewTables(addList)) return;
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
						} else {
							loadedTableNames = null;
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

	protected boolean handleNewTables(List<String> addList) {
		if (CollectionUtils.isNotEmpty(addList)) {
			List<String> loadedTableNames;
			final List<String> noPrimaryKeyTableNames = new ArrayList<>();
			obsLogger.info("Found new table(s): " + addList);
			List<TapTable> addTapTables = new ArrayList<>();
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			// Load new table schema
			if (obsLogger.isDebugEnabled()) {
				obsLogger.debug("Starting load new table(s) schema: {}", addList);
			}
			Function<TapTable, Boolean> filterTableByNoPrimaryKey = Optional.ofNullable(getNode()).map(node -> {
				if (node instanceof DatabaseNode) {
					DatabaseNode databaseNode = (DatabaseNode) node;
					if ("expression".equals(databaseNode.getMigrateTableSelectType())) {
						NoPrimaryKeyTableSelectType type = NoPrimaryKeyTableSelectType.parse(databaseNode.getNoPrimaryKeyTableSelectType());
						switch (type) {
							case HasKeys:
								// filter no hove primary key tables
								return (Function<TapTable, Boolean>) tapTable -> Optional.ofNullable(tapTable.primaryKeys()).map(Collection::isEmpty).orElse(true);
							case NoKeys:
								// filter has primary key tables
								return (Function<TapTable, Boolean>) tapTable -> !Optional.ofNullable(tapTable.primaryKeys()).map(Collection::isEmpty).orElse(true);
							default:
								break;
						}
					}
				}
				return null;
			}).orElse(tapTable -> false);
			LoadSchemaRunner.pdkDiscoverSchema(getConnectorNode(), addList, tapTable -> {
				if (filterTableByNoPrimaryKey.apply(tapTable)) {
					logger.warn("Ignore DDL no primary key table '{}'", tapTable.getId());
					noPrimaryKeyTableNames.add(tapTable.getId());
					return;
				}
				addTapTables.add(tapTable);
			});
			if (obsLogger.isDebugEnabled()) {
				if (CollectionUtils.isNotEmpty(addTapTables)) {
					addTapTables.forEach(tapTable -> obsLogger.debug("Loaded new table schema: {}", tapTable));
				}
			}
			obsLogger.info("Load new table(s) schema finished, loaded schema count: {}", addTapTables.size());
			loadedTableNames = addTapTables.stream().map(TapTable::getId).collect(Collectors.toList());
			List<String> missingTableNames = new ArrayList<>();
			addList.forEach(tableName -> {
				if (!noPrimaryKeyTableNames.contains(tableName) && !loadedTableNames.contains(tableName)) {
					missingTableNames.add(tableName);
				}
			});
			if (CollectionUtils.isNotEmpty(missingTableNames)) {
				obsLogger.warn("It is expected to load {} new table models, and {} table models no longer exist and will be ignored. The table name(s) that does not exist: {}",
						addList.size(), missingTableNames.size(), missingTableNames);
			}
			if (CollectionUtils.isNotEmpty(loadedTableNames)) {
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
						return true;
					}

					tapdataEvents.add(tapdataEvent);
				}
				if (!isRunning()) {
					return true;
				}
				tapdataEvents.forEach(this::enqueue);
				this.newTables.addAll(loadedTableNames);
				AspectUtils.executeAspect(new SourceDynamicTableAspect()
						.dataProcessorContext(getDataProcessorContext())
						.type(SourceDynamicTableAspect.DYNAMIC_TABLE_TYPE_ADD)
						.tables(loadedTableNames)
						.tapdataEvents(tapdataEvents));
				if (this.endSnapshotLoop.get()) {
					obsLogger.info("It is detected that the snapshot reading has ended, and the reading thread will be restarted");
					// Restart source runner
					if (null != sourceRunner) {
						this.sourceRunnerFirstTime.set(false);
						restartPdkConnector();
					} else {
						String error = "Source runner is null";
						errorHandle(new RuntimeException(error), error);
						return true;
					}
				}
			}
		}
		return false;
	}

	abstract void startSourceRunner();

	synchronized void restartPdkConnector() {
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
		this.sourceRunner = AsyncUtils.createThreadPoolExecutor(String.format("Source-Runner-table-changed-%s[%s]", getNode().getName(), getNode().getId()), 2, connectorOnTaskThreadGroup, TAG);
		initAndStartSourceRunner();
	}

	@NotNull
	public List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events) {
		return wrapTapdataEvent(events, SyncStage.INITIAL_SYNC, null);
	}

	@NotNull
	protected List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events, SyncStage syncStage, Object offsetObj) {
		int size = events.size();
		List<TapdataEvent> tapdataEvents = new ArrayList<>(size + 1);
		List<TapEvent> eventCache = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			TapEvent tapEvent = events.get(i);
			if (null == tapEvent.getTime()) {
				throw new NodeException("Invalid TapEvent, `TapEvent.time` should be NonNUll").context(getProcessorBaseContext()).event(tapEvent);
			}
			TapEvent tapEventCache = cdcDelayCalculation.filterAndCalcDelay(tapEvent, times -> AspectUtils.executeAspect(SourceCDCDelayAspect.class, () -> new SourceCDCDelayAspect().delay(times).dataProcessorContext(dataProcessorContext)));
			eventCache.add(tapEventCache);
			boolean isLast = i == (size - 1);
			TapdataEvent tapdataEvent;
			tapdataEvent = wrapTapdataEvent(tapEventCache, syncStage, offsetObj, isLast);
			if (null == tapdataEvent) {
				continue;
			}
			tapdataEvents.add(tapdataEvent);
		}
		return tapdataEvents;
	}

	protected TapdataEvent wrapTapdataEvent(TapEvent tapEvent, SyncStage syncStage, Object offsetObj, boolean isLast) {
		try {
			if (SyncStage.CDC == syncStage) {
				// Fixed #149167 in 2023-10-29: CDC batch events not full consumed cause loss data
				//todo: Remove lastStreamOffset if need add transaction in streamReadConsumer.
				if (isLast) lastStreamOffset.set(offsetObj);
				return wrapSingleTapdataEvent(tapEvent, syncStage, lastStreamOffset.get(), isLast);
			} else {
				return wrapSingleTapdataEvent(tapEvent, syncStage, offsetObj, isLast);
			}
		} catch (Throwable throwable) {
			throw new NodeException("Error wrap TapEvent, event: " + tapEvent + ", error: " + throwable
					.getMessage(), throwable)
					.context(getDataProcessorContext())
					.event(tapEvent);
		}
	}

	private TapdataEvent wrapSingleTapdataEvent(TapEvent tapEvent, SyncStage syncStage, Object offsetObj, boolean isLast) {
		TapdataEvent tapdataEvent = null;
		switch (sourceMode) {
			case NORMAL:
				tapdataEvent = new TapdataEvent();
				break;
			case LOG_COLLECTOR:
				tapdataEvent = new TapdataShareLogEvent();
				Connections connections = dataProcessorContext.getConnections();
				Node<?> node = getNode();
				if (node.isLogCollectorNode()) {
					List<Connections> connectionList = ShareCdcUtil.getConnectionIds(getNode(), ids -> {
						Query connectionQuery = new Query(where("_id").in(ids));
						connectionQuery.fields().include("config").include("pdkHash");
						List<Connections> connectionsList = clientMongoOperator.find(connectionQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
						for (Connections conn : connectionsList) {
							connectionMap.put(conn.getId(), conn);
						}
						return connectionsList;
					}, connectionMap);
					if (CollectionUtils.isNotEmpty(connectionList) && tapEvent instanceof TapBaseEvent) {
						TapdataEvent finalTapdataEvent = tapdataEvent;
						TapBaseEvent baseEvent = (TapBaseEvent) tapEvent;
						connectionList.forEach(connection -> {
							LogCollectorNode logCollectorNode = (LogCollectorNode) node;
							List<String> logNameSpaces = logCollectorNode.getLogCollectorConnConfigs().get(connection.getId()).getNamespace();
							List<String> tableNames = logCollectorNode.getLogCollectorConnConfigs().get(connection.getId()).getTableNames();
							if (logNameSpaces.contains(baseEvent.getNamespaces().get(0)) && tableNames.contains(baseEvent.getTableId())) {
								finalTapdataEvent.addInfo(TapdataEvent.CONNECTION_ID_INFO_KEY, connection.getId());
							}

						});
					} else {
						if (null != connections) {
							tapdataEvent.addInfo(TapdataEvent.CONNECTION_ID_INFO_KEY, connections.getId());
						}
					}
				} else {
					if (null != connections) {
						tapdataEvent.addInfo(TapdataEvent.CONNECTION_ID_INFO_KEY, connections.getId());
					}
				}
				break;
		}
		tapdataEvent.setTapEvent(tapEvent);
		tapdataEvent.setSyncStage(syncStage);
		if (tapEvent instanceof TapRecordEvent) {
			if (SyncStage.INITIAL_SYNC == syncStage) {
				if (isLast && !StringUtils.equalsAnyIgnoreCase(dataProcessorContext.getTaskDto().getSyncType(),
						TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
					Map<String, Object> batchOffsetObj = (Map<String, Object>) syncProgress.getBatchOffsetObj();
					Map<String, Object> newMap = new HashMap<>();
					try {
						MapUtil.deepCloneMap(batchOffsetObj, newMap);
					} catch (IllegalAccessException | InstantiationException e) {
						throw new RuntimeException("Deep clone batch offset map failed: " + e.getMessage(), e);
					}
					tapdataEvent.setBatchOffset(newMap);
					tapdataEvent.setStreamOffset(syncProgress.getStreamOffsetObj());
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


			tapdataEvent.setStreamOffset(offsetObj);
			tapdataEvent.setSourceTime(((TapDDLEvent) tapEvent).getReferenceTime());
			if (sourceMode.equals(SourceMode.NORMAL)) {
				handleSchemaChange(tapEvent);
			}
		}
		return tapdataEvent;
	}

	protected void handleSchemaChange(TapEvent tapEvent) {
		String tableId = ((TapDDLEvent) tapEvent).getTableId();
		TapTable tapTable;
		// Modify schema by ddl event
		if (tapEvent instanceof TapCreateTableEvent) {
			tapTable = ((TapCreateTableEvent) tapEvent).getTable();
		} else {
			try {
				tapTable = processorBaseContext.getTapTableMap().get(tableId);
				ddlSchemaHandler().updateSchemaByDDLEvent((TapDDLEvent) tapEvent, tapTable);
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
			DAGDataServiceImpl dagDataService = initDagDataService(transformerWsMessageDto);
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
				metadata.setTableAttr(metadata.getTableAttr());
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

	protected DAGDataServiceImpl initDagDataService(TransformerWsMessageDto transformerWsMessageDto) {
		return new DAGDataServiceImpl(transformerWsMessageDto);
	}

	protected DDLSchemaHandler ddlSchemaHandler() {
		return InstanceFactory.bean(DDLSchemaHandler.class);
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
				TapdataEvent event = this.tapEventFilter.handle(tapdataEvent);
				if (eventQueue.offer(event, 3, TimeUnit.SECONDS)) {
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

	protected void setDefaultRowSizeMap() {
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

	protected Long doBatchCountFunction(BatchCountFunction batchCountFunction, TapTable table) {
		AtomicReference<Long> counts = new AtomicReference<>();

		executeDataFuncAspect(
				TableCountFuncAspect.class,
				() -> new TableCountFuncAspect().dataProcessorContext(getDataProcessorContext()).start(),
				tableCountFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_BATCH_COUNT, createPdkMethodInvoker().runnable(() -> {
					try {
						counts.set(batchCountFunction.count(getConnectorNode().getConnectorContext(), table));

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

	protected AutoCloseable doAsyncTableCount(BatchCountFunction batchCountFunction, String tableName) {
		if (null == batchCountFunction) return () -> {
		};

		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
			// ignore table count if task stop
			if (!isRunning()) return;

			AtomicReference<TaskDto> task = new AtomicReference<>(getDataProcessorContext().getTaskDto());
			AtomicReference<Node<?>> node = new AtomicReference<>(getDataProcessorContext().getNode());
			String name = String.format("InitialSyncTableCount-%s(%s)-%s(%s)-%s",
					task.get().getName(), task.get().getId().toHexString(), node.get().getName(), node.get().getId(), tableName);
			Thread.currentThread().setName(name);

			TapTable table = getDataProcessorContext().getTapTableMap().get(tableName);
			Long counts = doBatchCountFunction(batchCountFunction, table);
			obsLogger.info("Query table '{}' counts: {}", tableName, counts);
			if (null == snapshotRowSizeMap) {
				snapshotRowSizeMap = new HashMap<>();
			}
			snapshotRowSizeMap.putIfAbsent(tableName, counts);
		});

		return () -> {
			try {
				future.join();
			} catch (Exception e) {
				if (isRunning()) {
					obsLogger.warn("Query '{}' snapshot row size failed: {}", tableName, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					return;
				}
				obsLogger.info("Cancel query '{}' snapshot row size with task stopped.", tableName);
			}
		};
	}

	protected boolean isPollingCDC(Node<?> node) {
		return !SyncTypeEnum.INITIAL_SYNC.equals(syncType) && node instanceof TableNode && "polling".equals(((TableNode) node).getCdcMode());
	}

	@Override
	public void doClose() throws TapCodeException {
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

	public void startSourceConsumer() {
		while (isRunning()) {
			try {
				TapdataEvent dataEvent;
				AtomicBoolean isPending = new AtomicBoolean();
				if (pendingEvent != null) {
					dataEvent = pendingEvent;
					pendingEvent = null;
					isPending.compareAndSet(false, true);
				} else {
					try {
						dataEvent = eventQueue.poll(5, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						break;
					}
					isPending.compareAndSet(true, false);
				}

				if (dataEvent != null) {
					if (!isPending.get()) {
						TapCodecsFilterManager codecsFilterManager = getConnectorNode().getCodecsFilterManager();
						TapEvent tapEvent = dataEvent.getTapEvent();
						tapRecordToTapValue(tapEvent, codecsFilterManager);
					}
					if (!offer(dataEvent)) {
						pendingEvent = dataEvent;
						continue;
					}
					Optional.ofNullable(getSnapshotProgressManager())
							.ifPresent(s -> s.incrementEdgeFinishNumber(TapEventUtil.getTableId(dataEvent.getTapEvent())));
				}
			} catch (Throwable e) {
				errorHandle(e, "start source consumer failed: " + e.getMessage());
				break;
			}
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

	protected boolean hasMergeNode() {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		List<Node> nodes = taskDto.getDag().getNodes();
		return null != nodes.stream().filter(n -> n instanceof MergeTableNode
				|| n instanceof JoinProcessorNode
				|| n instanceof UnionProcessorNode).findFirst().orElse(null);
	}

	private void initDynamicAdjustMemory() {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		if (null == taskDto.getDynamicAdjustMemoryUsage()) {
			taskDto.setDynamicAdjustMemoryUsage(false);
		}
		if (taskDto.getDynamicAdjustMemoryUsage()) {
			taskDto.setDynamicAdjustMemoryThresholdByte(null != taskDto.getDynamicAdjustMemoryThresholdByte() && taskDto.getDynamicAdjustMemoryThresholdByte() > 0L ? taskDto.getDynamicAdjustMemoryThresholdByte() : DEFAULT_RAM_THRESHOLD_BYTE);
			taskDto.setDynamicAdjustMemorySampleRate(null != taskDto.getDynamicAdjustMemorySampleRate() && taskDto.getDynamicAdjustMemorySampleRate() > 0L ? taskDto.getDynamicAdjustMemorySampleRate() : DEFAULT_SAMPLE_RATE);
			DynamicAdjustMemoryContext dynamicAdjustMemoryContext = DynamicAdjustMemoryContext.create()
					.sampleRate(taskDto.getDynamicAdjustMemorySampleRate())
					.ramThreshold(taskDto.getDynamicAdjustMemoryThresholdByte())
					.taskDto(taskDto)
					.minQueueSize(MIN_QUEUE_SIZE);
			this.dynamicAdjustMemoryService = new DynamicAdjustMemoryImpl(dynamicAdjustMemoryContext);
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
}

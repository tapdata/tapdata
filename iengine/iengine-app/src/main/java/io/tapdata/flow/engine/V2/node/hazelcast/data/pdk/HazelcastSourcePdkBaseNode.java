package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.util.ReUtil;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.LockUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
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
import com.tapdata.tm.commons.exception.NoPrimaryKeyException;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import com.tapdata.tm.commons.util.NoPrimaryKeyTableSelectType;
import com.tapdata.tm.commons.util.NoPrimaryKeyVirtualField;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.aspect.SourceCDCDelayAspect;
import io.tapdata.aspect.SourceDynamicTableAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TableCountFuncAspect;
import io.tapdata.aspect.supervisor.DataNodeThreadGroupAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.concurrent.SimpleConcurrentProcessorImpl;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.common.concurrent.exception.ConcurrentProcessorApplyException;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.ddl.DDLFilter;
import io.tapdata.flow.engine.V2.ddl.DDLSchemaHandler;
import io.tapdata.flow.engine.V2.entity.SyncProgressNodeType;
import io.tapdata.flow.engine.V2.filter.FilterUtil;
import io.tapdata.flow.engine.V2.filter.TargetTableDataEventFilter;
import io.tapdata.flow.engine.V2.monitor.Monitor;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.monitor.impl.PartitionTableMonitor;
import io.tapdata.flow.engine.V2.monitor.impl.TableMonitor;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryContext;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryService;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.impl.DynamicAdjustMemoryImpl;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.sharecdc.ShareCDCOffset;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.inspect.AutoRecovery;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.QueryPartitionTablesByParentName;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import io.tapdata.supervisor.TaskNodeInfo;
import io.tapdata.supervisor.TaskResourceSupervisorManager;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
	public static final int BATCH_SCAN_PARTITION_SUB_TABLE_BATCH = 10;
	public static final String SOURCE_TO_TAP_VALUE_CONCURRENT_PROP_KEY = "SOURCE_TO_TAP_VALUE_CONCURRENT";
	public static final String SOURCE_TO_TAP_VALUE_CONCURRENT_NUM_PROP_KEY = "SOURCE_TO_TAP_VALUE_CONCURRENT_NUM";
	public static final int BATCH_SIZE = 200;
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkBaseNode.class);
	protected SyncProgress syncProgress;
	protected ThreadPoolExecutorEx sourceRunner;
	protected ThreadPoolExecutorEx toTapValueRunner;
	protected ScheduledExecutorService tableMonitorResultHandler;
	protected SnapshotProgressManager snapshotProgressManager;
	protected int sourceQueueCapacity;
	protected int originalSourceQueueCapacity;
	private final TargetTableDataEventFilter tapEventFilter;
	final protected Map<String, TapTable> partitionTableSubMasterMap = new HashMap<>();;

	/**
	 * This is added as an async control center because pdk and jet have two different thread model. pdk thread is
	 * blocked when reading data from data source while jet using async when passing the event to next node.
	 */
	protected LinkedBlockingQueue<TapdataEvent> eventQueue;
	private final AtomicReference<Object> lastStreamOffset = new AtomicReference<>();
	protected StreamReadFuncAspect streamReadFuncAspect;
	protected TapdataEvent pendingEvent;
	protected List<TapdataEvent> pendingEvents;
	protected SourceMode sourceMode = SourceMode.NORMAL;
	protected TransformerWsMessageDto transformerWsMessageDto;
	protected DDLFilter ddlFilter;
	protected final ReentrantLock sourceRunnerLock;
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
	TaskResourceSupervisorManager taskResourceSupervisorManager = InstanceFactory.bean(TaskResourceSupervisorManager.class);
	private int toTapValueThreadNum;
	private TapCodecsFilterManager codecsFilterManagerSchemaEnforced;

	protected TapCodecsFilterManager defaultCodecsFilterManager;

	protected TapCodecsRegistry defaultCodecsRegistry = TapCodecsRegistry.create();
	private boolean toTapValueConcurrent;
	private boolean connectorNodeSchemaFree;
	private SimpleConcurrentProcessorImpl<List<TapdataEvent>, List<TapdataEvent>> toTapValueConcurrentProcessor;
	private int drainSize;
	private int toTapValueBatchSize;
	protected Boolean syncSourcePartitionTableEnable;
    protected final NoPrimaryKeyVirtualField noPrimaryKeyVirtualField = new NoPrimaryKeyVirtualField();
	protected List<String> loggedTables = new ArrayList<>();
	private List<Node<?>> targetDataNodes;

	public HazelcastSourcePdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.tapEventFilter = TargetTableDataEventFilter.create();
		this.sourceRunnerLock = new ReentrantLock(true);
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
        noPrimaryKeyVirtualField.init(getNode().getGraph());
        AutoRecovery.setEnqueueConsumer(getNode().getTaskId(), this::enqueue);
		ConcurrentHashSet<TaskNodeInfo> taskNodeInfos = taskResourceSupervisorManager.getTaskNodeInfos();
		ThreadGroup connectorOnTaskThreadGroup = getReuseOrNewThreadGroup(taskNodeInfos);
		if (needCdcDelay()) {
			this.cdcDelayCalculation = new CdcDelay();
		} else {
			this.cdcDelayCalculation = new CdcDelayDisable();
		}
		this.sourceRunner = AsyncUtils.createThreadPoolExecutor(String.format("Source-Runner-%s-%s[%s]", dataProcessorContext.getTaskDto().getId().toString(), getNode().getName(), getNode().getId()), 3, connectorOnTaskThreadGroup, TAG);
        initSyncPartitionTableEnable();
		this.sourceRunner.submitSync(() -> {
			super.doInit(context);
			try {
				createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
				AspectUtils.executeAspect(DataNodeThreadGroupAspect.class, () ->
						new DataNodeThreadGroupAspect(this.getNode(), associateId, Thread.currentThread().getThreadGroup())
								.dataProcessorContext(dataProcessorContext));
				connectorNodeInit(dataProcessorContext);
				obsLogger.info("Source connector({}) initialization completed", getNode().getName());
			} catch (Throwable e) {
				obsLogger.error("Source connector(" + getNode().getName() + ") initialization error: " + e.getMessage(), e);
				throw new NodeException(e).context(getProcessorBaseContext());
			}
            dataProcessorContext.getTapTableMap().forEach((id, table) -> noPrimaryKeyVirtualField.add(table));
			initSourceReadBatchSize();
			initSourceEventQueue();
			initSyncProgress();
			initDDLFilter();
			initTapEventFilter();
			initTableMonitor();
			initDynamicAdjustMemory();
			initSourceRunnerOnce();
			initTargetDataNodes();
			initAndStartSourceRunner();
			initTapCodecsFilterManager();
			initToTapValueConcurrent();
		});
	}

	private void initTargetDataNodes() {
		Node<?> node = getNode();
		targetDataNodes = GraphUtil.successors(node, n -> n instanceof TableNode || n instanceof DatabaseNode);
	}

	/**
     * Initialization: Whether the target has enabled synchronization of partition tables
     * */
    protected void initSyncPartitionTableEnable() {
        Node<?> node = getNode();
        this.syncSourcePartitionTableEnable =
				node instanceof DataParentNode &&
						Boolean.TRUE.equals(((DataParentNode<?>) node).getSyncSourcePartitionTableEnable());
		if (syncSourcePartitionTableEnable)
			obsLogger.info("Enable partition table support for source database");
	}

	protected void initToTapValueConcurrent() {
		toTapValueConcurrent = CommonUtils.getPropertyBool(SOURCE_TO_TAP_VALUE_CONCURRENT_PROP_KEY, false);
		toTapValueThreadNum = CommonUtils.getPropertyInt(SOURCE_TO_TAP_VALUE_CONCURRENT_NUM_PROP_KEY, Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
		if (Boolean.TRUE.equals(toTapValueConcurrent)) {
			this.toTapValueRunner = AsyncUtils.createThreadPoolExecutor(String.format("ToTapValue-Runner-%s[%s]", getNode().getName(), getNode().getId()), 1, TAG);
			toTapValueBatchSize = Math.max(1, readBatchSize / toTapValueThreadNum);
			toTapValueConcurrentProcessor = TapExecutors.createSimple(toTapValueThreadNum, 5, TAG);
			toTapValueConcurrentProcessor.start();
			this.toTapValueRunner.execute(this::concurrentToTapValueConsumer);
		}
	}

	protected void concurrentToTapValueConsumer() {
		try {
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			while (isRunning()) {
				int drain = Queues.drain(eventQueue, tapdataEvents, readBatchSize, 1L, TimeUnit.MILLISECONDS);
				if (drain > 0) {
					List<List<TapdataEvent>> partition = ListUtils.partition(tapdataEvents, toTapValueBatchSize);
					for (List<TapdataEvent> events : partition) {
						toTapValueConcurrentProcessor.runAsync(events, e -> {
							batchTransformToTapValue(e);
							return e;
						});
					}
					tapdataEvents = new ArrayList<>();
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	protected void initTapCodecsFilterManager() {
		ConnectorNode connectorNode = getConnectorNode();
		List<String> tags = connectorNode.getConnectorContext().getSpecification().getTags();
		this.connectorNodeSchemaFree = tags.contains("schema-free");
		this.codecsFilterManager = connectorNode.getCodecsFilterManager();
		this.codecsFilterManagerSchemaEnforced = connectorNode.getCodecsFilterManagerSchemaEnforced();
		this.defaultCodecsFilterManager = TapCodecsFilterManager.create(this.defaultCodecsRegistry);
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
			obsLogger.trace("Before the event is output to the target from source, it will automatically block field changes");
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

	protected void initSyncProgress() throws JsonProcessingException {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		Node node = getNode();
		Map<String, SyncProgress> allSyncProgress = foundAllSyncProgress(taskDto.getAttrs());
		this.syncProgress = foundNodeSyncProgress(allSyncProgress, SyncProgressNodeType.SOURCE);
		if (null == this.syncProgress) {
			obsLogger.trace("On the first run, the breakpoint will be initialized", node.getName());
		} else {
			obsLogger.info("Found exists breakpoint, will decode batch/stream offset", node.getName());
		}
		if (!StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(),
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			initBatchAndStreamOffset(taskDto);
			List<String> offsetLog = new ArrayList<>();
			if (null != syncProgress.getBatchOffsetObj()) {
				Map<String, Boolean> tableOffsetInfo = BatchOffsetUtil.getAllTableBatchOffsetInfo(syncProgress);
				if (!tableOffsetInfo.isEmpty())
					offsetLog.add(String.format("Use existing batch read offset: %s", JSONUtil.obj2Json(syncProgress.getBatchOffsetObj())));
			}
			if (null != syncProgress.getStreamOffsetObj()) {
				offsetLog.add(String.format("Use existing stream offset: %s", JSONUtil.obj2Json(syncProgress.getStreamOffsetObj())));
			}
			if (!offsetLog.isEmpty())
				obsLogger.info(String.join(", ", offsetLog));
		}
	}



	private void initSourceRunnerOnce() {
		this.endSnapshotLoop = new AtomicBoolean(false);
		this.transformerWsMessageDto = clientMongoOperator.findOne(new Query(),
				ConnectorConstant.TASK_COLLECTION + "/transformAllParam/" + processorBaseContext.getTaskDto().getId().toHexString(),
				TransformerWsMessageDto.class);
		this.sourceRunnerFirstTime = new AtomicBoolean(true);
		this.databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, dataProcessorContext.getConnections().getPdkHash());
	}

	protected void initAndStartSourceRunner() {
		this.lastStreamOffset.set(syncProgress.getStreamOffset());
		this.sourceRunnerFuture = this.sourceRunner.submit(this::startSourceRunner);
	}

	private void initSourceEventQueue() {
		this.sourceQueueCapacity = readBatchSize * SOURCE_QUEUE_FACTOR;
		this.originalSourceQueueCapacity = sourceQueueCapacity;
		this.eventQueue = new LinkedBlockingQueue<>(sourceQueueCapacity);
		obsLogger.trace("Source node \"{}\" event queue capacity: {}", getNode().getName(), sourceQueueCapacity);
	}

	private void initSourceReadBatchSize() {
		this.readBatchSize = DEFAULT_READ_BATCH_SIZE;
		this.increaseReadSize = DEFAULT_INCREASE_BATCH_SIZE;
		if (getNode() instanceof DataParentNode) {
			this.readBatchSize = Optional.ofNullable(((DataParentNode<?>) dataProcessorContext.getNode()).getReadBatchSize()).orElse(DEFAULT_READ_BATCH_SIZE);
			this.increaseReadSize = Optional.ofNullable(((DataParentNode<?>) dataProcessorContext.getNode()).getIncreaseReadSize()).orElse(DEFAULT_INCREASE_BATCH_SIZE);
		}
		this.drainSize = Math.max(1, readBatchSize / 2);
		obsLogger.trace("Source node \"{}\" read batch size: {}", getNode().getName(), readBatchSize);
	}

	protected void initDDLFilter() {
		Node<?> node = dataProcessorContext.getNode();
		if (node.isDataNode()) {
			List<String> disabledEvents = ((DataParentNode<?>) node).getDisabledEvents();
			DDLConfiguration ddlConfiguration = ((DataParentNode<?>) node).getDdlConfiguration();
			String ignoreDDLRules = ((DataParentNode<?>) node).getIgnoredDDLRules();
			this.ddlFilter = DDLFilter.create(disabledEvents, ddlConfiguration, ignoreDDLRules, obsLogger)
					.dynamicTableTest(this::checkDDLFilterPredicate);
		}
	}

	protected void initTableMonitor() throws Exception {
		Node<?> node = dataProcessorContext.getNode();
		if (node.isDataNode()) {
			boolean needDynamicTable = false;
			TableMonitor tableMonitor = null;
			if (needDynamicTable()) {
				//复制任务：正则表达式模式下动态新增表和动态新增分区子表
				needDynamicTable = true;
				Predicate<String> dynamicTableFilter = t -> ReUtil.isMatch(((DatabaseNode) node).getTableExpression(), t);
				tableMonitor = new TableMonitor(dataProcessorContext.getTapTableMap(),
						associateId, dataProcessorContext.getTaskDto(), dataProcessorContext.getSourceConn(), dynamicTableFilter);
			} else if (needDynamicPartitionTable()) {
				//复制任务和开发任务：普通模式下进行仅分区表的动态新增子表
				needDynamicTable = true;
				tableMonitor = new PartitionTableMonitor(dataProcessorContext.getTapTableMap(),
						associateId, dataProcessorContext.getTaskDto(), dataProcessorContext.getSourceConn(), t -> false);
			}

			if (needDynamicTable) {
				tableMonitor.withSyncSourcePartitionTableEnable(syncSourcePartitionTableEnable);
				this.newTables = new CopyOnWriteArrayList<>();
				this.removeTables = new CopyOnWriteArrayList<>();
				this.monitorManager.startMonitor(tableMonitor);
				this.tableMonitorResultHandler = new ScheduledThreadPoolExecutor(1);
				this.tableMonitorResultHandler.scheduleAtFixedRate(this::handleTableMonitorResult, 0L, PERIOD_SECOND_HANDLE_TABLE_MONITOR_RESULT, TimeUnit.SECONDS);
				logger.info("Handle dynamic add/remove table thread started, interval: " + PERIOD_SECOND_HANDLE_TABLE_MONITOR_RESULT + " seconds");
			}
		}
	}

	//@todo 只是判断开没开新增表，需要调整，是否验证表名称符合正则表达式需要另写方法
	protected boolean needDynamicTable() {
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
			return null != getTableNamesFunction;
		} else {
			return false;
		}
	}

	protected boolean needDynamicPartitionTable() {
    	if (!(dataProcessorContext.getNode() instanceof DatabaseNode)) return false;
		if (syncType.equals(SyncTypeEnum.INITIAL_SYNC)) {
			return false;
		}
    	if(!(Boolean.TRUE.equals(syncSourcePartitionTableEnable) && containsMasterPartitionTable())) {
    		return false;
		}
		QueryPartitionTablesByParentName queryPartitionTablesByParentName = getConnectorNode().getConnectorFunctions().getQueryPartitionTablesByParentName();
		return Objects.nonNull(queryPartitionTablesByParentName);
	}

	protected boolean checkDDLFilterPredicate(TapDDLEvent tapEvent) {
		final boolean subPartitionCreateTable = Boolean.TRUE.equals(syncSourcePartitionTableEnable)
				&& tapEvent instanceof TapCreateTableEvent
				&& ((TapCreateTableEvent)tapEvent).getTable().checkIsSubPartitionTable();
		if (subPartitionCreateTable) return true;

		final String tableId = tapEvent.getTableId();
		Node<?> node = dataProcessorContext.getNode();
		if (!(node instanceof DatabaseNode) || StringUtils.isNotEmpty(tableId)) {
			return true;
		}
		String expression = ((DatabaseNode) node).getTableExpression();
		if (StringUtils.isEmpty(expression)) return true;
		return ReUtil.isMatch(expression, tableId);
	}

	protected boolean containsMasterPartitionTable() {
		KVReadOnlyMap<TapTable> tableMap = getConnectorNode().getConnectorContext().getTableMap();
		Iterator<Entry<TapTable>> iterator = tableMap.iterator();
		while(iterator.hasNext()) {
			Entry<TapTable> next = iterator.next();
			if (next.getValue().checkIsMasterPartitionTable()) {
				return true;
			}
		}
		return false;
	}

	protected void initBatchAndStreamOffset(TaskDto taskDto) {
		if (syncProgress == null) {
			initBatchAndStreamOffsetFirstTime(taskDto);
		} else {
			readBatchAndStreamOffset(taskDto);
		}
	}

	protected void readBatchAndStreamOffset(TaskDto taskDto) {
		readBatchOffset(syncProgress);
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

	protected void initBatchAndStreamOffsetFirstTime(TaskDto taskDto) {
		syncProgress = new SyncProgress();
		// null present current
		Long offsetStartTimeMs = null;
		switch (syncType) {
			case INITIAL_SYNC_CDC:
				initStreamOffsetInitialAndCDC(offsetStartTimeMs);
				syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
				break;
			case INITIAL_SYNC:
				initStreamOffsetInitial();
				syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
				break;
			case CDC:
				offsetStartTimeMs = initStreamOffsetCDC(taskDto, offsetStartTimeMs);
				syncProgress.setSyncStage(SyncStage.CDC.name());
				break;
			default:
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
	public boolean complete() {
		try {
			if (firstComplete) {
				Thread.currentThread().setName(String.format("Source-Complete-%s[%s]", getNode().getName(), getNode().getId()));
				firstComplete = false;
			}
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			if (!isRunning()) {
				return true;
			}
			if (getNode().disabledNode()) {
				return true;
			}
			if (null != pendingEvents) {
				tapdataEvents = pendingEvents;
				pendingEvents = null;
			} else {
				if (Boolean.TRUE.equals(toTapValueConcurrent)) {
					try {
						tapdataEvents = toTapValueConcurrentProcessor.get(1L, TimeUnit.SECONDS);
					} catch (ConcurrentProcessorApplyException e) {
						// throw exception not include original events, local log file will include it
						logger.error("Concurrent transform to tap value failed, original events: {}", e.getOriginValue(), e.getCause());
						throw new Exception("Concurrent transform to tap value failed", e.getCause());
					}
				} else {
					if (null != eventQueue) {
						int drain = Queues.drain(eventQueue, tapdataEvents, drainSize, 100L, TimeUnit.MILLISECONDS);
						if (drain > 0) {
							batchTransformToTapValue(tapdataEvents);
						}
					}
				}
			}

			if (CollectionUtils.isNotEmpty(tapdataEvents)) {
				for (int i = 0; i < tapdataEvents.size(); i++) {
					TapdataEvent tapdataEvent = tapdataEvents.get(i);
					if (!hazelcastTaskNodeOffer.offer(tapdataEvent)) {
						pendingEvents = new ArrayList<>(tapdataEvents.subList(i, tapdataEvents.size()));
						return false;
					}
				}
			}

			if (sourceRunnerFuture != null && sourceRunnerFuture.isDone() && sourceRunnerFirstTime.get()
					&& null == pendingEvent && eventQueue.isEmpty() && checkAllTargetNodesFinishInitial()) {
				this.running.set(false);
			}

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			String errorMsg = String.format("Source sync failed: %s", e.getMessage());

			errorHandle(e, errorMsg);
		} finally {
			ThreadContext.clearAll();
		}

		return false;
	}

	private boolean checkAllTargetNodesFinishInitial() {
		boolean allTargetNodesFinishInitial = true;
		if (null == targetDataNodes) {
			return allTargetNodesFinishInitial;
		}
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		if (null == taskDto) {
			return true;
		}
		Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE.getTaskGlobalVariable(taskDto.getId().toHexString());
		for (Node<?> targetDataNode : targetDataNodes) {
			String key = String.join("_", TaskGlobalVariable.SOURCE_INITIAL_COUNTER_KEY, targetDataNode.getId());
			Object sourceInitialCounter = taskGlobalVariable.get(key);
			if(((AtomicInteger) sourceInitialCounter).intValue() > 0) {
				allTargetNodesFinishInitial = false;
				break;
			}
		}
		return allTargetNodesFinishInitial;
	}

	protected void batchTransformToTapValue(List<TapdataEvent> tapdataEvents) {
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			if (null == tapdataEvent.getTapEvent()) {
				continue;
			}
			transformToTapValue(tapdataEvent);
		}
	}

	private void transformToTapValue(TapdataEvent tapdataEvent) {
		if (SyncStage.CDC.equals(tapdataEvent.getSyncStage())
				|| connectorNodeSchemaFree
				|| TapInsertRecordEvent.TYPE != tapdataEvent.getTapEvent().getType()) {
			tapRecordToTapValue(tapdataEvent.getTapEvent(), codecsFilterManager);
		} else {
			TransformToTapValueResult transformToTapValueResult = tapRecordToTapValue(tapdataEvent.getTapEvent(), codecsFilterManagerSchemaEnforced);
			tapdataEvent.setTransformToTapValueResult(transformToTapValueResult);
		}
	}

	protected void handleTableMonitorResult() {
		Thread.currentThread().setName("Handle-Table-Monitor-Result-" + this.associateId);
		try {
			// Handle dynamic table change
			Object tableMonitor = monitorManager.getMonitorByType(MonitorManager.MonitorType.TABLE_MONITOR);
			if (null == tableMonitor) {
				tableMonitor = monitorManager.getMonitorByType(MonitorManager.MonitorType.PARTITION_TABLE_MONITOR);
			}

			if (tableMonitor instanceof TableMonitor) {
				((TableMonitor) tableMonitor).consume(tableResult -> {
					try {
						List<String> addList = tableResult.getAddList();
						List<String> removeList = tableResult.getRemoveList();
						if (CollectionUtils.isNotEmpty(addList) || CollectionUtils.isNotEmpty(removeList)) {
							LockUtil.runWithLock(
									this.sourceRunnerLock,
									() -> !isRunning(),
									() -> {
										// Remove from remove list if in add list
										if (CollectionUtils.isNotEmpty(addList)) {
											addList.forEach(tableName -> removeTables.remove(tableName));
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
												Optional.ofNullable(tapdataEvent).ifPresent(tapdataEvents::add);
											}
											tapdataEvents.forEach(this::enqueue);
											AspectUtils.executeAspect(new SourceDynamicTableAspect()
													.dataProcessorContext(getDataProcessorContext())
													.type(SourceDynamicTableAspect.DYNAMIC_TABLE_TYPE_REMOVE)
													.tables(removeList)
													.tapdataEvents(tapdataEvents));
										}

										// Handle new table(s)
										if (CollectionUtils.isNotEmpty(addList)) {
											handleNewTables(addList);
										}
									}
							);
						}
					} catch (Throwable throwable) {
						String error = "Handle table monitor result failed, result: " + tableResult + ", error: " + throwable.getMessage();
						throw new NodeException(error, throwable).context(getProcessorBaseContext());
					}
				});
			}
		} catch (Throwable throwable) {
			errorHandle(throwable, throwable.getMessage());
		}
	}

	protected boolean handleNewTables(List<String> addList) {
		if (CollectionUtils.isNotEmpty(addList)) {
			List<String> loadedTableNames;
			final List<String> noPrimaryKeyTableNames = new ArrayList<>();
			boolean needLog = false;
			if (!loggedTables.equals(addList)) {
				obsLogger.info("Found new table(s): " + addList);
				loggedTables.clear();
				loggedTables.addAll(addList);
				needLog = true;
			}
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
			final Map<TapTable, TapTable> masterAndNewMasterTable = new HashMap<>();
			Set<String> table = partitionTableSubMasterMap.values().stream().map(TapTable::getId).collect(Collectors.toSet());
			List<List<String>> partition = Lists.partition(addList, BATCH_SIZE);
            partition.forEach(part -> {
				List<String> batchList = new ArrayList<>(part);
				LoadSchemaRunner.pdkDiscoverSchema(getConnectorNode(), batchList, tapTable -> {
					if (table.contains(tapTable.getId())) return;
					if (Objects.nonNull(syncSourcePartitionTableEnable)
							&& !syncSourcePartitionTableEnable) {
						//开启了仅同步子表
						if (tapTable.checkIsMasterPartitionTable()) {
							//主表忽略
							batchList.remove(tapTable.getId());
							return;
						}
						if (tapTable.checkIsSubPartitionTable()) {
							//转成普通表处理
							tapTable.setPartitionMasterTableId(null);
							tapTable.setPartitionInfo(null);
						}
					}
					try {
						//主表已存在，需要新增表后更新主表的分区信息
						if (tapTable.checkIsMasterPartitionTable() && null != getConnectorNode().getConnectorContext().getTableMap().get(tapTable.getId())) {
							masterAndNewMasterTable.put(getConnectorNode().getConnectorContext().getTableMap().get(tapTable.getId()), tapTable);
							batchList.remove(tapTable.getId());
							return;
						}
					} catch (Exception e) {
						logger.debug("{} don't exists in table map", tapTable.getId());
					}

					if (filterTableByNoPrimaryKey.apply(tapTable)) {
						logger.warn("Ignore DDL no primary key table '{}'", tapTable.getId());
						noPrimaryKeyTableNames.add(tapTable.getId());
						return;
					}
					addTapTables.add(tapTable);
				});
			});
			if (obsLogger.isDebugEnabled()) {
				if (CollectionUtils.isNotEmpty(addTapTables)) {
					addTapTables.forEach(tapTable -> obsLogger.debug("Loaded new table schema: {}", tapTable));
				}
			}
			if (needLog) {
				obsLogger.trace("Load new table(s) schema finished, loaded schema count: {}", addTapTables.size());
			}
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
			if (CollectionUtils.isEmpty(loadedTableNames)) {
				return false;
			}

			for (TapTable addTapTable : addTapTables) {
				if (!isRunning()) {
					break;
				}
				if (checkSubPartitionTableHasBeCreated(addTapTable)) {
					loadedTableNames.remove(addTapTable.getId());
					continue;
				}

				mergeSubInfoIntoMasterTableIfNeed(addTapTable);

				TapCreateTableEvent tapCreateTableEvent = new TapCreateTableEvent();
				addTapTable.setLastUpdate(Optional.ofNullable(addTapTable.getLastUpdate()).orElse(System.currentTimeMillis()));
				tapCreateTableEvent.table(addTapTable);
				tapCreateTableEvent.setTableId(addTapTable.getId());
				tapCreateTableEvent.setPartitionMasterTableId(addTapTable.getPartitionMasterTableId());
				TapdataEvent tapdataEvent = wrapTapdataEvent(tapCreateTableEvent, SyncStage.valueOf(syncProgress.getSyncStage()), null, false);
				BatchOffsetUtil.updateBatchOffset(syncProgress, addTapTable.getId(), null, TableBatchReadStatus.RUNNING.name());
				tapdataEvent.setBatchOffset(syncProgress.getBatchOffsetObj());
				tapdataEvent.setSourceTime(System.currentTimeMillis());

				if (null == tapdataEvent) {
					String error = "Wrap create table tapdata event failed: " + addTapTable;
					errorHandle(new RuntimeException(error), error);
					return true;
				}

				tapdataEvents.add(tapdataEvent);
				noPrimaryKeyVirtualField.add(addTapTable);
			}
			if (!isRunning()) {
				return true;
			}
			tapdataEvents.forEach(this::enqueue);
			if (!loadedTableNames.isEmpty()) {
				this.newTables.addAll(loadedTableNames);
			}
			List<TapdataEvent> normalDDLEvents = tapdataEvents.stream()
					.filter(e -> {
						if (e.getTapEvent() instanceof TapCreateTableEvent
								&& ((TapCreateTableEvent) e.getTapEvent()).getTable().checkIsSubPartitionTable()) {
							loadedTableNames.remove(((TapCreateTableEvent) e.getTapEvent()).getTable().getId());
							return false;
						}
						return true;
					})
					.collect(Collectors.toList());
			AspectUtils.executeAspect(new SourceDynamicTableAspect()
					.dataProcessorContext(getDataProcessorContext())
					.type(SourceDynamicTableAspect.DYNAMIC_TABLE_TYPE_ADD)
					.tables(loadedTableNames)
					.tapdataEvents(normalDDLEvents));
			if (tapdataEvents.isEmpty()) return false;

			if (this.endSnapshotLoop.get()) {
				obsLogger.trace("It is detected that the snapshot reading has ended, and the reading thread will be restarted");
				// Restart source runner
				if (null != sourceRunner) {
					this.sourceRunnerFirstTime.set(false);
					newTables.forEach(id -> BatchOffsetUtil.updateBatchOffset(syncProgress, id, null, TableBatchReadStatus.RUNNING.name()));
					restartPdkConnector();
				} else {
					String error = "Source runner is null";
					errorHandle(new RuntimeException(error), error);
					return true;
				}
			}
		}
		return false;
	}

	protected void mergeSubInfoIntoMasterTableIfNeed(TapTable addTapTable) {
		if (!addTapTable.checkIsSubPartitionTable() || !Objects.nonNull(getConnectorNode())) {
			return;
		}
		String subTableId = addTapTable.getId();
		TapTable masterTable = getConnectorNode().getConnectorContext().getTableMap().get(addTapTable.getPartitionMasterTableId());
		TapPartition partitionInfo = addTapTable.getPartitionInfo();
		List<TapSubPartitionTableInfo> subPartitionTableInfo = partitionInfo.getSubPartitionTableInfo();
		List<TapSubPartitionTableInfo> masterPartitionTableInfo = Optional.ofNullable(masterTable.getPartitionInfo().getSubPartitionTableInfo()).orElse(new ArrayList<>());
		Map<String, TapSubPartitionTableInfo> tableInfoMap = masterPartitionTableInfo.stream().collect(Collectors.toMap(TapSubPartitionTableInfo::getTableName, info -> info));
		subPartitionTableInfo.stream().filter(info -> info.getTableName().equals(subTableId))
				.forEach(info -> {
					if (!tableInfoMap.containsKey(subTableId)) {
						masterPartitionTableInfo.add(info);
					}
				});
		masterTable.getPartitionInfo().setSubPartitionTableInfo(masterPartitionTableInfo);
	}

	protected boolean checkSubPartitionTableHasBeCreated(TapTable addTapTable) {
    	if (this.partitionTableSubMasterMap.containsKey(addTapTable.getId())) return true;
		if (addTapTable.checkIsSubPartitionTable() && Objects.nonNull(getConnectorNode())) {
			TapTable masterTable = getConnectorNode().getConnectorContext().getTableMap().get(addTapTable.getPartitionMasterTableId());
			if(Objects.isNull(masterTable)) {
				return false;
			}
			this.partitionTableSubMasterMap.put(addTapTable.getId(), masterTable);
			TapPartition partitionInfo = masterTable.getPartitionInfo();
			if (Objects.isNull(partitionInfo)) {
				return false;
			}
			List<TapSubPartitionTableInfo> subPartitionTableInfo = partitionInfo.getSubPartitionTableInfo();
			if (Objects.isNull(subPartitionTableInfo)) {
				return false;
			}
			Optional<TapSubPartitionTableInfo> first = subPartitionTableInfo.stream()
					.filter(info -> addTapTable.getId().equals(info.getTableName()))
					.findFirst();
			if (first.isPresent()) {
				return true;
			}
		}
		return false;
	}

	abstract void startSourceRunner();

	synchronized void restartPdkConnector() {
		logger.warn(this.associateId);
		if (null != getConnectorNode()) {
			//Release webhook waiting thread before stop connectorNode.
			if (streamReadFuncAspect != null) {
				streamReadFuncAspect.noMoreWaitRawData();
				streamReadFuncAspect = null;
			}
			Optional.ofNullable(this.sourceRunner).ifPresent(ExecutorService::shutdownNow);
			PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.STOP, () -> getConnectorNode().connectorStop(), TAG);
			PDKIntegration.releaseAssociateId(this.associateId);
			ConnectorNodeService.getInstance().removeConnectorNode(this.associateId);
			createPdkConnectorNode(dataProcessorContext, jetContext.hazelcastInstance());
			connectorNodeInit(dataProcessorContext);
			Monitor<?> monitor = monitorManager.getMonitorByType(MonitorManager.MonitorType.TABLE_MONITOR);
			if (monitor == null) {
				monitor = monitorManager.getMonitorByType(MonitorManager.MonitorType.PARTITION_TABLE_MONITOR);
			}
			if (monitor instanceof TableMonitor) {
				((TableMonitor) monitor).setAssociateId(this.associateId);
			}
			this.sourceRunner = AsyncUtils.createThreadPoolExecutor(String.format("Source-Runner-table-changed-%s[%s]", getNode().getName(), getNode().getId()), 2, connectorOnTaskThreadGroup, TAG);
			initAndStartSourceRunner();
		} else {
			String error = "Connector node is null";
			errorHandle(new RuntimeException(error), error);
		}
	}

	@NotNull
	public List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events) {
		return wrapTapdataEvent(events, SyncStage.INITIAL_SYNC, null);
	}

	@NotNull
	protected List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events, SyncStage syncStage, Object offsetObj) {
		int size = events.size();
		List<TapdataEvent> tapdataEvents = new ArrayList<>(size + 1);
		for (int i = 0; i < size; i++) {
			TapEvent tapEvent = events.get(i);
			if (null == tapEvent.getTime()) {
				throw new NodeException("Invalid TapEvent, `TapEvent.time` should be NonNUll").context(getProcessorBaseContext()).event(tapEvent);
			}
			TapEvent tapEventCache = cdcDelayCalculation.filterAndCalcDelay(tapEvent, times -> AspectUtils.executeAspect(SourceCDCDelayAspect.class, () -> new SourceCDCDelayAspect().delay(times).dataProcessorContext(dataProcessorContext)));
			boolean isLast = i == (size - 1);
			TapdataEvent tapdataEvent = wrapTapdataEvent(tapEventCache, syncStage, offsetObj, isLast);
			if (null == tapdataEvent) {
				continue;
			}
			tapdataEvents.add(tapdataEvent);
		}
//		tapdataEvents.add(new TapdataSourceBatchSplitEvent());
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

	protected TapdataEvent wrapSingleTapdataEvent(TapEvent tapEvent, SyncStage syncStage, Object offsetObj, boolean isLast) {
		TapdataEvent tapdataEvent = null;
		switch (sourceMode) {
			case NORMAL:
				tapdataEvent = new TapdataEvent();
				break;
			case LOG_COLLECTOR:
				if (tapEvent instanceof TapDDLUnknownEvent) {
					obsLogger.warn("DDL unknown event: " + tapEvent);
					return null;
				}
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
            TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;
			if (SyncStage.INITIAL_SYNC == syncStage) {
				if (isLast && !StringUtils.equalsAnyIgnoreCase(dataProcessorContext.getTaskDto().getSyncType(),
						TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
					tapdataEvent.setBatchOffset(BatchOffsetUtil.getTableOffsetInfo(syncProgress, recordEvent.getTableId()));
					tapdataEvent.setStreamOffset(syncProgress.getStreamOffsetObj());
					tapdataEvent.setSourceTime(syncProgress.getSourceTime());
				}
			} else if (SyncStage.CDC == syncStage) {
				tapdataEvent.setStreamOffset(offsetObj);
				if (null == recordEvent.getReferenceTime())
					throw new RuntimeException("Tap CDC event's reference time is null");
				tapdataEvent.setSourceTime(recordEvent.getReferenceTime());
			}
            try {
                if (!noPrimaryKeyVirtualField.addHashValue(recordEvent)) return null;
            } catch (NoPrimaryKeyException e) {
                if (NoPrimaryKeyException.CODE_INCOMPLETE_FIELDS == e.getCode()) {
                    obsLogger.warn("Table '{}' user lacks complete before information, and subsequent update and delete events will be ignored: {}", recordEvent.getTableId(), e.getMessage());
                    logger.warn("Table '{}' user lacks complete before information, and subsequent update and delete events will be ignored", recordEvent.getTableId(), e);
                } else {
                    obsLogger.warn("Table '{}' add hash filed failed, and subsequent update and delete events will be ignored: {}", recordEvent.getTableId(), e.getMessage());
                    logger.error("Table '{}' add hash filed failed, and subsequent update and delete events will be ignored", recordEvent.getTableId(), e);
                }
            }
        } else if (tapEvent instanceof HeartbeatEvent) {
			tapdataEvent = TapdataHeartbeatEvent.create(((HeartbeatEvent) tapEvent).getReferenceTime(), offsetObj);
		} else if (tapEvent instanceof TapDDLEvent) {
			obsLogger.trace("Source node received an ddl event: " + tapEvent);

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

	protected void setPartitionMasterTableId(TapTable tapTable, List<TapEvent> events) {
		if (null == syncSourcePartitionTableEnable
				|| !syncSourcePartitionTableEnable
				|| !tapTable.checkIsSubPartitionTable()) return;
		events.stream()
				.filter(TapRecordEvent.class::isInstance)
				.forEach(e -> {
					((TapRecordEvent) e).setPartitionMasterTableId(tapTable.getPartitionMasterTableId());
					TapEventUtil.swapTableIdAndMasterTableId(e);
				});
	}

	protected void setPartitionMasterTableId(List<TapEvent> events) {
		TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
		events.stream()
			.filter(TapRecordEvent.class::isInstance)
			.forEach(e -> {
				TapRecordEvent event = ((TapRecordEvent) e);
				String eventTableId = event.getTableId();
				TapTable tapTable;
				if (null == syncSourcePartitionTableEnable
						|| !syncSourcePartitionTableEnable) {
					//开启了忽略主表，仅同步子表（当普通表处理，不需要传递主表ID）
					return;
				}
				if (partitionTableSubMasterMap.containsKey(eventTableId)) {
					tapTable = partitionTableSubMasterMap.get(eventTableId);
					//子表：更改表ID为主表ID，masterId为子表ID，如果目标关闭了分区同步，需要撤回这个操作｜
					//     源上会用tableid获取表后去做类型转换或者啥操作，拿子表的话旧有问题，换成getPartitionMasterTableId去拿主表修改范围太大了
					Optional.ofNullable(tapTable.getId()).ifPresent(event::setTableId);
					Optional.ofNullable(eventTableId).ifPresent(event::setPartitionMasterTableId);
				} else {
					tapTable = tapTableMap.get(eventTableId);
					if (Objects.nonNull(tapTable) && tapTable.checkIsSubPartitionTable()) {
						Optional.ofNullable(tapTable.getPartitionMasterTableId()).ifPresent(event::setPartitionMasterTableId);
					}
				}
			});
	}

	protected void setPartitionMasterTableId(TapRecordEvent event, String partitionMasterTableId) {
		if (null == syncSourcePartitionTableEnable || !syncSourcePartitionTableEnable) return;
		Optional.ofNullable(partitionMasterTableId).ifPresent(event::setPartitionMasterTableId);
		TapEventUtil.swapTableIdAndMasterTableId(event);
	}

	protected void handleSchemaChange(TapEvent tapEvent) {
		String tableId = ((TapDDLEvent) tapEvent).getTableId();
		TapTable tapTable;
		// Modify schema by ddl event
		if (tapEvent instanceof TapCreateTableEvent) {
			tapTable = ((TapCreateTableEvent) tapEvent).getTable();
		} else if (tapEvent instanceof TapClearTableEvent) {
			return;
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
			DAGDataServiceImpl dagDataService = initDagDataService(transformerWsMessageDto);
			String qualifiedName;
			Map<String, List<Message>> errorMessage;
			if (tapEvent instanceof TapCreateTableEvent) {
				boolean isSubPartition = Boolean.TRUE.equals(syncSourcePartitionTableEnable) && tapTable.checkIsSubPartitionTable();
				if (isSubPartition) {
					obsLogger.trace("Sync sub table's [{}] create table ddl, will add update master table [{}] metadata", tapTable.getId(), tapTable.getPartitionMasterTableId());
					String masterTableMetadataQualifiedName = dataProcessorContext.getTapTableMap().getQualifiedName(tapTable.getPartitionMasterTableId());
					MetadataInstancesDto masterTableMetadata = dagDataService.getMetadata(masterTableMetadataQualifiedName);
					if (masterTableMetadata.getId() == null) {
						masterTableMetadata.setId(masterTableMetadata.getOldId());
					}
					masterTableMetadata.setPartitionInfo(dataProcessorContext.getTapTableMap().get(tapTable.getPartitionMasterTableId()).getPartitionInfo());
					transformerWsMessageDto.getMetadataInstancesDtoList().add(masterTableMetadata);
					updateMetadata.put(masterTableMetadata.getId().toHexString(), masterTableMetadata);
				}
				qualifiedName = dagDataService.createNewTable(dataProcessorContext.getSourceConn().getId(), tapTable, processorBaseContext.getTaskDto().getId().toHexString());
				obsLogger.trace("Create new table in memory, qualified name: " + qualifiedName);
				dataProcessorContext.getTapTableMap().putNew(tapTable.getId(), tapTable, qualifiedName);
				errorMessage = dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions());
				TaskDto taskDto = dagDataService.getTaskById(processorBaseContext.getTaskDto().getId().toHexString());
				taskDto.setDag(dag);
				MetadataInstancesDto metadata = dagDataService.getMetadata(qualifiedName);
				if (null == metadata.getId()) {
					metadata.setId(new ObjectId());
				}
				transformerWsMessageDto.getMetadataInstancesDtoList().add(metadata);
                obsLogger.trace("Create new table schema transform finished: " + tapTable);
                if(!isSubPartition) {
					obsLogger.trace("Sync sub table's [{}] create table ddl,, will ignore sub table's metadata", tapTable.getId());
					insertMetadata.add(metadata);
				}
			} else if (tapEvent instanceof TapDropTableEvent) {
				qualifiedName = dataProcessorContext.getTapTableMap().getQualifiedName(((TapDropTableEvent) tapEvent).getTableId());
				obsLogger.trace("Drop table in memory qualified name: " + qualifiedName);
				dagDataService.dropTable(qualifiedName);
				transformerWsMessageDto.getMetadataInstancesDtoList().stream().filter(m -> Objects.equals(m.getQualifiedName(), qualifiedName)).findFirst()
						.ifPresent(m -> transformerWsMessageDto.getMetadataInstancesDtoList().remove(m));
				errorMessage = dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions());
				removeMetadata.add(qualifiedName);
				obsLogger.trace("Drop table schema transform finished");
			} else {
				BatchOffsetUtil.updateBatchOffsetWhenTableRename(syncProgress, tapEvent);
				qualifiedName = dataProcessorContext.getTapTableMap().getQualifiedName(tableId);
				obsLogger.trace("Alter table in memory, qualified name: " + qualifiedName);
				dagDataService.coverMetaDataByTapTable(qualifiedName, tapTable);
				errorMessage = dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions());
				MetadataInstancesDto metadata = dagDataService.getMetadata(qualifiedName);
				if (metadata.getId() == null) {
					metadata.setId(metadata.getOldId());
				}
				transformerWsMessageDto.getMetadataInstancesDtoList().stream().filter(m -> Objects.equals(m.getQualifiedName(), qualifiedName)).findFirst()
						.ifPresent(m -> transformerWsMessageDto.getMetadataInstancesDtoList().remove(m));
				transformerWsMessageDto.getMetadataInstancesDtoList().add(metadata);
				metadata.setTableAttr(metadata.getTableAttr());
				updateMetadata.put(metadata.getId().toHexString(), metadata);
				obsLogger.trace("Alter table schema transform finished");
			}

			List<MetadataInstancesDto> metadataInstancesDtoList = transformerWsMessageDto.getMetadataInstancesDtoList();
			Map<String, String> qualifiedNameIdMap = metadataInstancesDtoList.stream()
					.sorted((m1,m2) -> {
						if (null == m1.getLastUpdate()) return -1;
						if (null == m2.getLastUpdate()) return 1;
						return m1.getLastUpdate().compareTo(m2.getLastUpdate());
					}).collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName,
							m -> null == m.getId() ? "" : m.getId().toHexString(),
							(existingValue, newValue) -> existingValue));
			tapEvent.addInfo(QUALIFIED_NAME_ID_MAP_INFO_KEY, qualifiedNameIdMap);
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
				TapdataEvent event = tapdataEvent;
				if (SyncStage.CDC.name().equals(syncProgress.getSyncStage())) {
					event = this.tapEventFilter.handle(tapdataEvent);
				}
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
			asyncCountTable(batchCountFunction, tableList);
		} else {
			doCountSynchronously(batchCountFunction, tableList, false);
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
	protected void doCountSynchronously(BatchCountFunction batchCountFunction, List<String> tableList, boolean displayTableListFirst) {
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
											long count;
											if (displayTableListFirst) {
												count = -1;
											} else {
												count = batchCountFunction.count(getConnectorNode().getConnectorContext(), table);
												if (null == snapshotRowSizeMap) {
													snapshotRowSizeMap = new HashMap<>();
												}
												snapshotRowSizeMap.putIfAbsent(tableName, count);
											}
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
	protected void doTableNameSynchronously(BatchCountFunction batchCountFunction, List<String> tableList) {
		doCountSynchronously(batchCountFunction, tableList, true);
		asyncCountTable(batchCountFunction, tableList);
	}

	protected void asyncCountTable(BatchCountFunction batchCountFunction, List<String> tableList) {
		logger.info("Start to asynchronously count the size of rows for the source table(s)");
		AtomicReference<TaskDto> task = new AtomicReference<>(dataProcessorContext.getTaskDto());
		AtomicReference<Node<?>> node = new AtomicReference<>(dataProcessorContext.getNode());
		snapshotRowSizeThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
		CompletableFuture.runAsync(() -> {
					String name = String.format("Snapshot-Row-Size-Query-Thread-%s(%s)-%s(%s)",
							task.get().getName(), task.get().getId().toHexString(), node.get().getName(), node.get().getId());
					Thread.currentThread().setName(name);

					doCountSynchronously(batchCountFunction, tableList, false);
				}, snapshotRowSizeThreadPool)
				.whenComplete((v, e) -> {
					if (null != e) {
						obsLogger.warn("Query snapshot row size failed: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					} else {
						obsLogger.trace("Query snapshot row size completed: " + node.get().getName() + "(" + node.get().getId() + ")");
					}
					ExecutorUtil.shutdown(this.snapshotRowSizeThreadPool, 10L, TimeUnit.SECONDS);
				});
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
			obsLogger.warn("Node can not support batchCountFunction");
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
			obsLogger.trace("Query table '{}' counts: {}", tableName, counts);
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
				obsLogger.trace("Cancel query '{}' snapshot row size with task stopped.", tableName);
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
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(toTapValueRunner).ifPresent(ExecutorService::shutdownNow), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(toTapValueConcurrentProcessor).ifPresent(SimpleConcurrentProcessorImpl::close), TAG);
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
			obsLogger.info("Enable dynamic memory adjustment");
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

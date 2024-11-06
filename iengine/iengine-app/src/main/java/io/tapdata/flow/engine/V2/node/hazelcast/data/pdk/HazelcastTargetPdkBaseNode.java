package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.ConcurrentHashSet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Queues;
import com.hazelcast.jet.core.Inbox;
import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.autoinspect.utils.GZIPUtil;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.DmlPolicyEnum;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageResult;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.NoPrimaryKeyVirtualField;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.supervisor.DataNodeThreadGroupAspect;
import io.tapdata.aspect.taskmilestones.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TapdataEventException;
import io.tapdata.error.TaskTargetProcessorExCode_15;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.common.TapdataEventsRunner;
import io.tapdata.flow.engine.V2.exactlyonce.ExactlyOnceUtil;
import io.tapdata.flow.engine.V2.exactlyonce.write.CheckExactlyOnceWriteEnableResult;
import io.tapdata.flow.engine.V2.exactlyonce.write.ExactlyOnceWriteCleaner;
import io.tapdata.flow.engine.V2.exactlyonce.write.ExactlyOnceWriteCleanerEntity;
import io.tapdata.flow.engine.V2.exception.TapExactlyOnceWriteExCode_22;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderController;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderService;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.PartitionConcurrentProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.KeysPartitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.PartitionResult;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.Partitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.PartitionKeySelector;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.TapEventPartitionKeySelector;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryConstant;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryExCode_25;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.flow.engine.V2.util.TargetTapEventFilter;
import io.tapdata.inspect.AutoRecovery;
import io.tapdata.metric.collector.ISyncMetricCollector;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.supervisor.TaskNodeInfo;
import io.tapdata.supervisor.TaskResourceSupervisorManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:58
 **/
public abstract class HazelcastTargetPdkBaseNode extends HazelcastPdkBaseNode {
    private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
    public static final long DEFAULT_TARGET_BATCH_INTERVAL_MS = 1000;
    public static final int DEFAULT_TARGET_BATCH = 1000;
    public static final int TARGET_QUEUE_FACTOR = 2;
    public static final int COMPRESS_STREAM_OFFSET_STRING_LENGTH_THRESHOLD = 100;
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
    protected Map<String, List<String>> concurrentWritePartitionMap;
    private PartitionConcurrentProcessor initialPartitionConcurrentProcessor;
    private PartitionConcurrentProcessor cdcPartitionConcurrentProcessor;
    private LinkedBlockingQueue<TapdataEvent> tapEventQueue;
    private LinkedBlockingQueue<TapdataEvent> tapEventProcessQueue;
    private final Object saveSnapshotLock = new Object();
    private ThreadPoolExecutorEx queueConsumerThreadPool;
    private boolean inCdc = false;
    protected int targetBatch;
    protected long targetBatchIntervalMs;
    private TargetTapEventFilter targetTapEventFilter;
    protected final List<String> exactlyOnceWriteTables = new ArrayList<>();
    protected final ConcurrentHashMap<String, List<String>> exactlyOnceWriteNeedLookupTables = new ConcurrentHashMap<>();
    protected CheckExactlyOnceWriteEnableResult checkExactlyOnceWriteEnableResult;

    private final List<ExactlyOnceWriteCleanerEntity> exactlyOnceWriteCleanerEntities = new ArrayList<>();
    protected int originalWriteQueueCapacity;
    protected int writeQueueCapacity;
    protected final int[] dynamicAdjustQueueLock = new int[0];
    private final ScheduledExecutorService flushOffsetExecutor;
    protected ISyncMetricCollector syncMetricCollector;

    protected Boolean unwindProcess = false;
    protected boolean illegalDateAcceptable = false;
    protected ConcurrentHashMap<String, Boolean> everHandleTapTablePrimaryKeysMap;
    TaskResourceSupervisorManager taskResourceSupervisorManager = InstanceFactory.bean(TaskResourceSupervisorManager.class);
    private TapCodecsFilterManager codecsFilterManagerForBatchRead;
	protected boolean syncTargetPartitionTableEnable;

    public HazelcastTargetPdkBaseNode(DataProcessorContext dataProcessorContext) {
        super(dataProcessorContext);
        initQueueConsumerThreadPool();
        //threadPoolExecutorEx = AsyncUtils.createThreadPoolExecutor("Target-" + getNode().getName() + "@task-" + dataProcessorContext.getTaskDto().getName(), 1, new ConnectorOnTaskThreadGroup(dataProcessorContext), TAG);
        flushOffsetExecutor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r);
            thread.setName(String.format("Flush-Offset-Thread-%s(%s)-%s(%s)", dataProcessorContext.getTaskDto().getName(), dataProcessorContext.getTaskDto().getId().toHexString(), getNode().getName(), getNode().getId()));
            return thread;
        });
        TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.RUNNING);
    }

    @Override
    protected void doInit(@NotNull Context context) throws TapCodeException {
        syncMetricCollector = ISyncMetricCollector.init(dataProcessorContext);
		queueConsumerThreadPool.submitSync(() -> {
			super.doInit(context);
			createPdkAndInit(context);
			everHandleTapTablePrimaryKeysMap = new ConcurrentHashMap<>();
			initExactlyOnceWriteIfNeed();
			initTargetVariable();
			initTargetQueueConsumer();
			initTargetConcurrentProcessorIfNeed();
			initTapEventFilter();
			initIllegalDateAcceptable();
            initSyncProgressMap();
			flushOffsetExecutor.scheduleWithFixedDelay(this::saveToSnapshot, 10L, 10L, TimeUnit.SECONDS);
			initCodecsFilterManager();
		});
		Thread.currentThread().setName(String.format("Target-Process-%s[%s]", getNode().getName(), getNode().getId()));
		checkUnwindConfiguration();
		initSyncPartitionTableEnable();
	}

	/**
	 * Initialization: Whether the target has enabled synchronization of partition tables
	 * */
	protected void initSyncPartitionTableEnable() {
		Node<?> node = getNode();
		this.syncTargetPartitionTableEnable = node instanceof DataParentNode && Boolean.TRUE.equals(((DataParentNode<?>) node).getSyncTargetPartitionTableEnable());
	}

    protected void initSyncProgressMap() {
		Map<String, SyncProgress> allSyncProgress = foundAllSyncProgress(dataProcessorContext.getTaskDto().getAttrs());
		for (Map.Entry<String, SyncProgress> entry : allSyncProgress.entrySet()) {
			readBatchOffset(entry.getValue());
		}
		syncProgressMap.putAll(allSyncProgress);
	}

	protected void initCodecsFilterManager() {
        Optional.ofNullable(getConnectorNode()).ifPresent(connectorNode -> codecsFilterManager = connectorNode.getCodecsFilterManager());
        Optional.ofNullable(getConnectorNode()).ifPresent(connectorNode -> codecsFilterManagerForBatchRead = connectorNode.getCodecsFilterManagerSchemaEnforced());
    }

    @Override
    protected boolean isRunning() {
        return super.isRunning();
    }

    @Override
    protected void doInitWithDisableNode(@NotNull Context context) throws TapCodeException {
        queueConsumerThreadPool.submitSync(() -> {
            super.doInitWithDisableNode(context);
            createPdkAndInit(context);
        });
        Thread.currentThread().setName(String.format("Target-Process-%s[%s]", getNode().getName(), getNode().getId()));
        everHandleTapTablePrimaryKeysMap = new ConcurrentHashMap<>();
    }

    protected void initQueueConsumerThreadPool() {
        ConcurrentHashSet<TaskNodeInfo> taskNodeInfos = taskResourceSupervisorManager.getTaskNodeInfos();
        ThreadGroup connectorOnTaskThreadGroup = getReuseOrNewThreadGroup(taskNodeInfos);
        queueConsumerThreadPool = AsyncUtils.createThreadPoolExecutor(String.format("Target-Queue-Consumer-%s[%s]@task-%s", getNode().getName(), getNode().getId(), dataProcessorContext.getTaskDto().getName()), 2, connectorOnTaskThreadGroup, TAG);
    }

    protected void initExactlyOnceWriteIfNeed() {
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
        boolean create = createTable(exactlyOnceTable, new AtomicBoolean(), true);
        if (create) {
            obsLogger.info("Create exactly once write cache table: {}", exactlyOnceTable);
            CreateIndexFunction createIndexFunction = connectorFunctions.getCreateIndexFunction();
            TapCreateIndexEvent indexEvent = createIndexEvent(exactlyOnceTable.getId(), exactlyOnceTable.getIndexList());
            PDKInvocationMonitor.invoke(
                    getConnectorNode(), PDKMethod.TARGET_CREATE_INDEX,
                    () -> createIndexFunction.createIndex(getConnectorNode().getConnectorContext(), exactlyOnceTable, indexEvent), TAG, buildErrorConsumer(exactlyOnceTable.getId()));
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

    protected void checkUnwindConfiguration() {
        TaskDto taskDto = dataProcessorContext.getTaskDto();
        taskDto.getDag().getNodes().forEach(node -> {
            if (node instanceof UnwindProcessNode) this.unwindProcess = true;
        });
        if (this.unwindProcess && getNode() instanceof TableNode) {
            DmlPolicy dmlPolicy = ((TableNode) getNode()).getDmlPolicy();
            if (null != dmlPolicy) {
                DmlPolicyEnum insertPolicy = ((TableNode) getNode()).getDmlPolicy().getInsertPolicy();
                if (null != insertPolicy && !insertPolicy.equals(DmlPolicyEnum.just_insert)) {
                    dmlPolicy.setInsertPolicy(DmlPolicyEnum.just_insert);
                    obsLogger.warn("The node write strategy using Unwind must be just_insert,Will automatically modify the write policy.");
                }
            } else {
                DmlPolicy policy = new DmlPolicy();
                policy.setInsertPolicy(DmlPolicyEnum.just_insert);
                policy.setUpdatePolicy(DmlPolicyEnum.ignore_on_nonexists);
                ((TableNode) getNode()).setDmlPolicy(policy);
                obsLogger.warn("The node write strategy using Unwind must be just_insert,Will automatically modify the write policy.");
            }
        }
    }

    protected void doCreateTable(TapTable tapTable, AtomicReference<TapCreateTableEvent> tapCreateTableEvent, Runnable runnable) {
		TapTable finalTapTable = new TapTable();
		handleTapTablePrimaryKeys(tapTable);
		BeanUtil.copyProperties(tapTable, finalTapTable);
		if(unwindProcess){
			ignorePksAndIndices(finalTapTable, null);
		}
		tapCreateTableEvent.set(createTableEvent(finalTapTable));
		masterTableId(tapCreateTableEvent.get(), tapTable);
		runnable.run();
		clientMongoOperator.insertOne(Collections.singletonList(finalTapTable),
				ConnectorConstant.CONNECTION_COLLECTION + "/load/part/tables/" + dataProcessorContext.getTargetConn().getId());
	}

	protected boolean createPartitionTable(CreatePartitionTableFunction createPartitionTableFunction,
										   AtomicBoolean succeed,
										   TapTable tapTable,
										   boolean init,
										   AtomicReference<TapCreateTableEvent> tapCreateTableEvent) {
		doCreateTable(tapTable, tapCreateTableEvent, () -> {
			final ConnectorNode connectorNode = getConnectorNode();
			final TapConnectorContext connectorContext = connectorNode.getConnectorContext();
			final TapCreateTableEvent createTableEvent = tapCreateTableEvent.get();
			executeDataFuncAspect(CreateTableFuncAspect.class, () -> new CreateTableFuncAspect()
					.createTableEvent(createTableEvent)
					.setInit(init)
					.connectorContext(connectorContext)
					.dataProcessorContext(dataProcessorContext)
					.start(), (createTableFuncAspect ->
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.CREATE_PARTITION_TABLE_FUNCTION, () -> {
						final CreateTableOptions createTableOptions = createPartitionTableFunction.createTable(connectorContext, createTableEvent);
						succeed.set(!createTableOptions.getTableExists());
						Optional.ofNullable(createTableFuncAspect).ifPresent(aspect -> aspect.createTableOptions(createTableOptions));
					}, TAG, buildErrorConsumer(createTableEvent.getTableId()))));
		});
		return true;
	}

	protected boolean createSubPartitionTable(CreatePartitionSubTableFunction createSubPartitionTableFunction,
										   AtomicBoolean succeed,
										   TapTable tapTable,
										   boolean init,
										   AtomicReference<TapCreateTableEvent> tapCreateTableEvent) {
		final ConnectorNode connectorNode = getConnectorNode();
		final TapConnectorContext connectorContext = connectorNode.getConnectorContext();
		final String subTableId = tapTable.getId();
		doCreateTable(tapTable, tapCreateTableEvent, () -> {
			final TapCreateTableEvent createTableEvent = tapCreateTableEvent.get();
			executeDataFuncAspect(CreateTableFuncAspect.class, () -> new CreateTableFuncAspect()
					.createTableEvent(createTableEvent)
					.setInit(init)
					.connectorContext(connectorContext)
					.dataProcessorContext(dataProcessorContext)
					.start(), (createTableFuncAspect ->
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.CREATE_PARTITION_SUB_TABLE_FUNCTION, () -> {
						final CreateTableOptions createTableOptions = createSubPartitionTableFunction.createSubPartitionTable(
									connectorContext,
									createTableEvent,
									subTableId
								);
						succeed.set(!Boolean.TRUE.equals(createTableOptions.getTableExists()));
						Optional.ofNullable(createTableFuncAspect).ifPresent(aspect -> aspect.createTableOptions(createTableOptions));
					}, TAG, buildErrorConsumer(createTableEvent.getTableId()))));
		});
		return true;
	}

	protected boolean createTable(TapTable tapTable, AtomicBoolean succeed,boolean init) {
		if (getNode().disabledNode()) {
			obsLogger.info("Target node has been disabled, task will skip: create table");
			return false;
		}
		if (!this.syncTargetPartitionTableEnable && tapTable.checkIsSubPartitionTable()) {
			obsLogger.info("Target node not enable partition, task will skip: create subpartition table {}",
					tapTable.getId());
			return false;
		}
		AtomicReference<TapCreateTableEvent> tapCreateTableEvent = new AtomicReference<>();
		boolean createdTable;
		boolean createPartitionTable;
		boolean createSubPartitionTable;
		try {
			CreateTableFunction createTableFunction = getConnectorNode().getConnectorFunctions().getCreateTableFunction();
			CreateTableV2Function createTableV2Function = getConnectorNode().getConnectorFunctions().getCreateTableV2Function();
			createdTable = createTableV2Function != null || createTableFunction != null;

			CreatePartitionTableFunction createPartitionTableFunction = getConnectorNode().getConnectorFunctions().getCreatePartitionTableFunction();
			CreatePartitionSubTableFunction createPartitionSubTableFunction = getConnectorNode().getConnectorFunctions().getCreatePartitionSubTableFunction();
			createPartitionTable = this.syncTargetPartitionTableEnable
					&& tapTable.checkIsMasterPartitionTable()
					&& Objects.nonNull(createPartitionTableFunction);
			createSubPartitionTable = tapTable.checkIsSubPartitionTable()
					&& Objects.nonNull(createPartitionSubTableFunction);
            if (createSubPartitionTable && !this.syncTargetPartitionTableEnable) {
                obsLogger.warn("Target has be close partition table sync, create sub partition table [{}] be ignore", tapTable.getId());
                return false;
			}
			if (createSubPartitionTable && null != tapTable.getPartitionInfo() && tapTable.getPartitionInfo().isInvalidType()) {
				obsLogger.warn("Target has be not support invalid partition sub table, create sub partition table [{}] be ignore {}", tapTable.getId(), tapTable.getPartitionInfo().getInvalidMsg());
				return false;
			}
			if (createPartitionTable && null != tapTable.getPartitionInfo() && tapTable.getPartitionInfo().isInvalidType()) {
				obsLogger.warn("Target can not support to create invalid partition master table [{}] be ignore {}, may create as a normal table", tapTable.getId(), tapTable.getPartitionInfo().getInvalidMsg());
				createPartitionTable = false;
			}

			if (createPartitionTable) {
				obsLogger.info("Will create master partition table [{}] to target, init sub partition list: {}",
						tapTable.getId(),
						Optional.ofNullable(tapTable.getPartitionInfo().getSubPartitionTableInfo()).orElse(new ArrayList<>())
								.stream().map(TapSubPartitionTableInfo::getTableName).collect(Collectors.toList()));
				return createPartitionTable(createPartitionTableFunction, succeed, tapTable, init, tapCreateTableEvent);
			} else if (createSubPartitionTable) {
				obsLogger.info("Will create sub partition table [{}] to target, master table is: {}", tapTable.getId(), tapTable.getPartitionMasterTableId());
				return createSubPartitionTable(createPartitionSubTableFunction, succeed, tapTable, init, tapCreateTableEvent);
			} else if (createdTable) {
				doCreateTable(tapTable, tapCreateTableEvent, () ->
					executeDataFuncAspect(CreateTableFuncAspect.class, () -> new CreateTableFuncAspect()
							.createTableEvent(tapCreateTableEvent.get())
							.setInit(init)
							.connectorContext(getConnectorNode().getConnectorContext())
							.dataProcessorContext(dataProcessorContext)
							.start(), (createTableFuncAspect ->
							PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TARGET_CREATE_TABLE, () -> {
								if (createTableV2Function != null) {
									CreateTableOptions createTableOptions = createTableV2Function.createTable(getConnectorNode().getConnectorContext(), tapCreateTableEvent.get());
									succeed.set(!createTableOptions.getTableExists());
									if (createTableFuncAspect != null)
										createTableFuncAspect.createTableOptions(createTableOptions);
								} else {
									createTableFunction.createTable(getConnectorNode().getConnectorContext(), tapCreateTableEvent.get());
								}
							}, TAG,buildErrorConsumer(tapCreateTableEvent.get().getTableId()))))
                );
			} else {
				// only execute start function aspect so that it would be cheated as input
				AspectUtils.executeAspect(new CreateTableFuncAspect()
						.createTableEvent(tapCreateTableEvent.get())
						.setInit(init)
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext).state(NewFieldFuncAspect.STATE_START));
				clientMongoOperator.insertOne(Collections.singletonList(tapTable),
						ConnectorConstant.CONNECTION_COLLECTION + "/load/part/tables/" + dataProcessorContext.getTargetConn().getId());
			}
        } catch (Throwable throwable) {
            Throwable matched = CommonUtils.matchThrowable(throwable, TapCodeException.class);
            if (null != matched) {
                throw (TapCodeException) matched;
            } else {
                throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_TABLE_FAILED, "Table model: " + tapTable, throwable)
                        .addEvent(tapCreateTableEvent.get());
            }
        }
        return createdTable;
    }

    protected void initTargetQueueConsumer() {
        this.queueConsumerThreadPool.submit(this::queueConsume);
        this.queueConsumerThreadPool.submit(this::processQueueConsume);
        obsLogger.debug("Initialize target event handler complete");
    }

    protected void initTargetConcurrentProcessorIfNeed() {
        if (getNode() instanceof DataParentNode) {
            DataParentNode<?> dataParentNode = (DataParentNode<?>) getNode();
            final Boolean initialConcurrentInConfig = dataParentNode.getInitialConcurrent();
            this.concurrentWritePartitionMap = dataParentNode.getConcurrentWritePartitionMap();
            Function<TapEvent, List<String>> partitionKeyFunction = new Function<TapEvent, List<String>>() {
                private final Set<String> warnTag = new HashSet<>();

                @Override
                public List<String> apply(TapEvent tapEvent) {
                    final String tgtTableName = getTgtTableNameFromTapEvent(tapEvent);
                    if (null != concurrentWritePartitionMap) {
                        List<String> fields = concurrentWritePartitionMap.get(tgtTableName);
                        if (null != fields && !fields.isEmpty()) {
                            return new ArrayList<>(fields);
                        }
                        if (!warnTag.contains(tgtTableName)) {
                            warnTag.add(tgtTableName);
                            obsLogger.warn("Not found partition fields of table '{}', use logic primary key.", tgtTableName);
                        }
                    }
                    TapTable tapTable = dataProcessorContext.getTapTableMap().get(tgtTableName);
                    handleTapTablePrimaryKeys(tapTable);
                    return new ArrayList<>(tapTable.primaryKeys(true));
                }
            };

            if (initialConcurrentInConfig != null) {
                this.initialConcurrentWriteNum = dataParentNode.getInitialConcurrentWriteNum() != null ? dataParentNode.getInitialConcurrentWriteNum() : 8;
                this.initialConcurrent = initialConcurrentInConfig && initialConcurrentWriteNum > 1;
                if (initialConcurrentInConfig) {
                    this.initialPartitionConcurrentProcessor = initInitialConcurrentProcessor(
                            initialConcurrentWriteNum,
                            new Partitioner<TapdataEvent, List<Object>>() {
                                final Random random = new Random();

                                @Override
                                public PartitionResult<TapdataEvent> partition(int partitionSize, TapdataEvent event, List<Object> partitionValue) {
                                    return new PartitionResult<>(random.nextInt(partitionSize), event);
                                }
                            }
                    );
                    this.initialPartitionConcurrentProcessor.start();
                }
            }
            final Boolean cdcConcurrentInConfig = dataParentNode.getCdcConcurrent();
            if (cdcConcurrentInConfig != null) {
                this.cdcConcurrentWriteNum = dataParentNode.getCdcConcurrentWriteNum() != null ? dataParentNode.getCdcConcurrentWriteNum() : 4;
                this.cdcConcurrent = isCDCConcurrent(cdcConcurrentInConfig);
                if (this.cdcConcurrent) {
                    this.cdcPartitionConcurrentProcessor = initCDCConcurrentProcessor(cdcConcurrentWriteNum, partitionKeyFunction);
                    this.cdcPartitionConcurrentProcessor.start();
                }
            }
        }
    }

    protected boolean isCDCConcurrent(Boolean cdcConcurrent) {
        cdcConcurrent = cdcConcurrent && cdcConcurrentWriteNum > 1;
        List<? extends Node<?>> predecessors = getNode().predecessors();
        for (Node<?> predecessor : predecessors) {
            if (predecessor instanceof MergeTableNode || predecessor instanceof UnwindProcessNode) {
                obsLogger.info("CDC concurrent write is disabled because the node has a merge table node or unwind process node");
                cdcConcurrent = false;
            }
        }
        return cdcConcurrent;
    }

    protected void initTargetVariable() {
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
        writeQueueCapacity = new BigDecimal(targetBatch).multiply(new BigDecimal(TARGET_QUEUE_FACTOR)).setScale(0, RoundingMode.HALF_UP).intValue();
        this.originalWriteQueueCapacity = writeQueueCapacity;
        this.tapEventQueue = new LinkedBlockingQueue<>(writeQueueCapacity);
        this.tapEventProcessQueue = new LinkedBlockingQueue<>(writeQueueCapacity);
        obsLogger.debug("Initialize target write queue complete, capacity: {}", writeQueueCapacity);
    }

    protected void createPdkAndInit(@NotNull Context context) {
        if (getNode() instanceof TableNode || getNode() instanceof DatabaseNode) {
            try {
                createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
                connectorNodeInit(dataProcessorContext);
            } catch (Throwable e) {
                throw new NodeException(e).context(getProcessorBaseContext());
            }
        }
    }

    protected void initTapEventFilter() {
        this.targetTapEventFilter = TargetTapEventFilter.create();
        //this.targetTapEventFilter.addFilter(new DeleteConditionFieldFilter());
    }

    @Override
    final public void process(int ordinal, @NotNull Inbox inbox) {
        if (getNode().disabledNode()) {
            return;
        }
        try {
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
                        enqueue(this.tapEventQueue, tapdataEvent);
                        if (tapdataEvent instanceof TapdataAdjustMemoryEvent) {
                            if (((TapdataAdjustMemoryEvent) tapdataEvent).needAdjust()) {
                                synchronized (this.dynamicAdjustQueueLock) {
                                    obsLogger.info("{}The target node enters the waiting phase until the queue adjustment is completed", DynamicAdjustMemoryConstant.LOG_PREFIX);
                                    this.dynamicAdjustQueueLock.wait();
                                }
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
            if (!preClassName.isEmpty() && !preClassName.equals(currClassName)) {
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

    protected void processTargetEvents(List<TapdataEvent> tapdataEvents) {
        for (TapdataEvent consumeEvent : tapdataEvents) {
            if (consumeEvent.getTapEvent() instanceof TapRecordEvent) {
                TapRecordEvent tapRecordEvent = (TapRecordEvent) consumeEvent.getTapEvent();
                String targetTableName = getTgtTableNameFromTapEvent(tapRecordEvent);
                replaceIllegalDateWithNullIfNeed(tapRecordEvent);
                TransformToTapValueResult transformToTapValueResult = consumeEvent.getTransformToTapValueResult();
                if (null != transformToTapValueResult) {
                    fromTapValue(TapEventUtil.getBefore(tapRecordEvent), codecsFilterManagerForBatchRead, targetTableName, transformToTapValueResult.getBeforeTransformedToTapValueFieldNames());
                    fromTapValue(TapEventUtil.getAfter(tapRecordEvent), codecsFilterManagerForBatchRead, targetTableName, transformToTapValueResult.getAfterTransformedToTapValueFieldNames());
                } else {
                    fromTapValue(TapEventUtil.getBefore(tapRecordEvent), codecsFilterManager, targetTableName);
                    fromTapValue(TapEventUtil.getAfter(tapRecordEvent), codecsFilterManager, targetTableName);
                }
                fromTapValueMergeInfo(consumeEvent);
            } else if (consumeEvent.getTapEvent() instanceof TapDDLEvent) {
                // send count down latch and await
                TapdataCountDownLatchEvent tapdataCountDownLatchEvent = TapdataCountDownLatchEvent.create(1);
                enqueue(this.tapEventProcessQueue, tapdataCountDownLatchEvent);
                obsLogger.info("The target node received dll event({}). Wait for all previous events to be processed", consumeEvent.getTapEvent());
                while (isRunning()) {
                    try {
                        if (tapdataCountDownLatchEvent.getCountDownLatch().await(1L, TimeUnit.SECONDS)) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                updateMemoryFromDDLInfoMap(consumeEvent);
                obsLogger.info("The target node refreshes the memory model according to the ddl event({})", consumeEvent.getTapEvent());
            }
            enqueue(this.tapEventProcessQueue, consumeEvent);
        }
    }

    protected void enqueue(LinkedBlockingQueue<TapdataEvent> tapEventQueue, TapdataEvent event) {
        while (isRunning()) {
            try {
                if (tapEventQueue.offer(event, 1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    protected void queueConsume() {
        try {
            AspectUtils.executeAspect(DataNodeThreadGroupAspect.class, () ->
                    new DataNodeThreadGroupAspect(this.getNode(), associateId, Thread.currentThread().getThreadGroup())
                            .dataProcessorContext(dataProcessorContext));
            drainAndRun(tapEventQueue, targetBatch, targetBatchIntervalMs, TimeUnit.MILLISECONDS, this::processTargetEvents);
        } catch (Throwable e) {
            executeAspect(WriteErrorAspect.class, () -> new WriteErrorAspect().dataProcessorContext(dataProcessorContext).error(e));
            Throwable throwableWrapper;
            Throwable tapCodeEx = CommonUtils.matchThrowable(e, TapCodeException.class);
            if (!(tapCodeEx instanceof TapCodeException)) {
                throwableWrapper = new TapCodeException(TaskTargetProcessorExCode_15.UNKNOWN_ERROR, e);
            } else {
                throwableWrapper = tapCodeEx;
            }
            errorHandle(throwableWrapper, null);
        } finally {
            ThreadContext.clearAll();
        }
    }

    protected void processQueueConsume() {
        try {
            drainAndRun(tapEventProcessQueue, targetBatch, targetBatchIntervalMs, TimeUnit.MILLISECONDS, tapdataEvents -> {
                dispatchTapdataEvents(
                        tapdataEvents,
                        consumeEvents -> {
                            if (consumeEvents.size() == 1 && consumeEvents.get(0) instanceof TapdataAdjustMemoryEvent) {
                                handleTapdataEvents(consumeEvents);
                                return;
                            }
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
            });
        } catch (Exception e) {
            executeAspect(WriteErrorAspect.class, () -> new WriteErrorAspect().dataProcessorContext(dataProcessorContext).error(e));
            Throwable matchThrowable = CommonUtils.matchThrowable(e, TapCodeException.class);
            if (null == matchThrowable) {
                matchThrowable = new TapCodeException(TaskTargetProcessorExCode_15.UNKNOWN_ERROR, e);
            }
            errorHandle(matchThrowable);
        }
    }

    protected void drainAndRun(BlockingQueue<TapdataEvent> queue, int elementsNum, long timeout, TimeUnit timeUnit, TapdataEventsRunner tapdataEventsRunner) {
        List<TapdataEvent> tapdataEvents = new ArrayList<>();
        while (isRunning()) {
            try {
                int drain = Queues.drain(queue, tapdataEvents, elementsNum, timeout, timeUnit);
                if (drain > 0) {
                    tapdataEventsRunner.run(tapdataEvents);
                    tapdataEvents = new ArrayList<>();
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void initialProcessEvents(List<TapdataEvent> initialEvents, boolean async) {

        if (CollectionUtils.isNotEmpty(initialEvents)) {
            if (initialConcurrent && null != this.initialPartitionConcurrentProcessor && this.initialPartitionConcurrentProcessor.isRunning()) {
                this.initialPartitionConcurrentProcessor.process(initialEvents, async);
            } else {
                this.handleTapdataEvents(initialEvents);
            }
        }
    }

    private void cdcProcessEvents(List<TapdataEvent> cdcEvents) {
        if (CollectionUtils.isNotEmpty(cdcEvents)) {
            if (cdcConcurrent && null != this.cdcPartitionConcurrentProcessor && this.cdcPartitionConcurrentProcessor.isRunning()) {
                this.cdcPartitionConcurrentProcessor.process(cdcEvents, true);
            } else {
                splitDDL2NewBatch(cdcEvents, this::handleTapdataEvents);
            }
        }
    }

    protected void splitDDL2NewBatch(List<TapdataEvent> cdcEvents, Consumer<List<TapdataEvent>> subListConsumer) {
        int beginIndex = 0;
        int len = cdcEvents.size();
        for (int i = 0; i < len; i++) {
            if (null != cdcEvents.get(i) && cdcEvents.get(i).getTapEvent() instanceof TapDDLEvent) {
                if (beginIndex < i) {
                    subListConsumer.accept(cdcEvents.subList(beginIndex, i));
                }
                beginIndex = i + 1;
                subListConsumer.accept(Collections.singletonList(cdcEvents.get(i)));
            }
        }

        if (0 == beginIndex) {
            subListConsumer.accept(cdcEvents);
        } else if (beginIndex != len) {
            subListConsumer.accept(cdcEvents.subList(beginIndex, len));
        }
    }

    protected void handleTapdataEvents(List<TapdataEvent> tapdataEvents) {
        AtomicReference<TapdataEvent> lastTapdataEvent = new AtomicReference<>();
        List<TapEvent> tapEvents = new ArrayList<>();
        List<TapRecordEvent> exactlyOnceWriteCache = new ArrayList<>();
        List<TapdataShareLogEvent> tapdataShareLogEvents = new ArrayList<>();

        initAndGetExactlyOnceWriteLookupList();
        AtomicBoolean hasExactlyOnceWriteCache = new AtomicBoolean(false);
        for (TapdataEvent tapdataEvent : tapdataEvents) {
            if (!isRunning()) {
                return;
            }
            handleTapdataEvent(tapEvents, tapdataShareLogEvents, lastTapdataEvent, hasExactlyOnceWriteCache, exactlyOnceWriteCache, tapdataEvent);
        }

        try {
            processTapEvents(tapdataEvents, tapEvents, hasExactlyOnceWriteCache);

            if (CollectionUtils.isNotEmpty(tapdataShareLogEvents)) {
                processShareLog(tapdataShareLogEvents);
            }

            flushOffsetByTapdataEventForNoConcurrent(lastTapdataEvent);
        } catch (Throwable throwable) {
            Throwable matched = CommonUtils.matchThrowable(throwable, TapCodeException.class);
            if (null != matched) {
                throw (TapCodeException) matched;
            } else {
                TapdataEventException eventException = new TapdataEventException(TaskTargetProcessorExCode_15.PROCESS_EVENTS_FAILED, throwable);
                if (null != lastTapdataEvent.get() && String.valueOf(lastTapdataEvent.get().getTapEvent()).length() < 1000000L) {
                    eventException.addEvent(lastTapdataEvent.get());
                }
                throw eventException;
            }
        }

        if (firstStreamEvent.get()) {
            executeAspect(new CDCHeartbeatWriteAspect().tapdataEvents(tapdataEvents).dataProcessorContext(dataProcessorContext));
        }
    }

    protected void handleTapdataEvent(List<TapEvent> tapEvents, List<TapdataShareLogEvent> tapdataShareLogEvents
            , AtomicReference<TapdataEvent> lastTapdataEvent, AtomicBoolean hasExactlyOnceWriteCache
            , List<TapRecordEvent> exactlyOnceWriteCache, TapdataEvent tapdataEvent) {
        try {
            Optional.ofNullable(tapdataEvent.getSyncStage()).ifPresent(this::handleAspectWithSyncStage);

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
            } else if (tapdataEvent instanceof TapdataAdjustMemoryEvent) {
                handleTapdataAdjustMemoryEvent((TapdataAdjustMemoryEvent) tapdataEvent);
			} else if (tapdataEvent instanceof TapdataCountDownLatchEvent) {
				Optional.of(tapdataEvent)
						.flatMap(event -> Optional.ofNullable(((TapdataCountDownLatchEvent) event).getCountDownLatch()))
						.ifPresent(CountDownLatch::countDown);
			} else if (tapdataEvent instanceof TapdataSourceBatchSplitEvent) {
				executeAspect(new WriteRecordFuncAspect().state(WriteRecordFuncAspect.BATCH_SPLIT).dataProcessorContext(dataProcessorContext));
			} else {
				handleTapdataEvent(tapEvents, hasExactlyOnceWriteCache, exactlyOnceWriteCache, lastTapdataEvent, tapdataEvent);
				if (tapdataEvent instanceof TapdataRecoveryEvent) {
					AutoRecovery.completed(getNode().getTaskId(), (TapdataRecoveryEvent) tapdataEvent);
				}
			}
        } catch (Throwable throwable) {
            throw new TapdataEventException(TaskTargetProcessorExCode_15.HANDLE_EVENTS_FAILED, throwable).addEvent(tapdataEvent);
        }
    }

    protected void processTapEvents(List<TapdataEvent> tapdataEvents, List<TapEvent> tapEvents, AtomicBoolean hasExactlyOnceWriteCache) {
        if (CollectionUtils.isEmpty(tapEvents)) return;

        if (Boolean.TRUE.equals(checkExactlyOnceWriteEnableResult.getEnable()) && hasExactlyOnceWriteCache.get()) {
            try {
                transactionBegin();
                processEvents(tapEvents);
                processExactlyOnceWriteCache(tapdataEvents);
                transactionCommit();
            } catch (Exception e) {
                transactionRollback();
                throw e;
            }
        } else {
            processEvents(tapEvents);
        }
    }

    protected void handleAspectWithSyncStage(SyncStage syncStage) {
        switch (syncStage) {
            case INITIAL_SYNC:
                if (firstBatchEvent.compareAndSet(false, true)) {
                    syncMetricCollector.snapshotBegin();
                    executeAspect(new SnapshotWriteBeginAspect().dataProcessorContext(dataProcessorContext));
                }
                break;
            case CDC:
                if (firstStreamEvent.compareAndSet(false, true)) {
                    syncMetricCollector.cdcBegin();
                    executeAspect(new CDCWriteBeginAspect().dataProcessorContext(dataProcessorContext));
                }
                break;
            default:
                break;
        }
    }

    protected void handleTapdataEvent(List<TapEvent> tapEvents, AtomicBoolean hasExactlyOnceWriteCache, List<TapRecordEvent> exactlyOnceWriteCache, AtomicReference<TapdataEvent> lastTapdataEvent, TapdataEvent tapdataEvent) throws JsonProcessingException {
        if (tapdataEvent.isDML()) {
            handleTapdataEventDML(tapEvents, hasExactlyOnceWriteCache, exactlyOnceWriteCache, lastTapdataEvent, tapdataEvent);
        } else if (tapdataEvent.isDDL()) {
            handleTapdataDDLEvent(tapdataEvent, tapEvents, lastTapdataEvent::set);
        } else {
            if (null != tapdataEvent.getTapEvent()) {
                obsLogger.warn("Tap event type does not supported: " + tapdataEvent.getTapEvent().getClass() + ", will ignore it");
            }
        }
    }

    protected void handleTapdataEventDML(List<TapEvent> tapEvents, AtomicBoolean hasExactlyOnceWriteCache, List<TapRecordEvent> exactlyOnceWriteCache, AtomicReference<TapdataEvent> lastTapdataEvent, TapdataEvent tapdataEvent) throws JsonProcessingException {
        TapRecordEvent tapRecordEvent = handleTapdataRecordEvent(tapdataEvent);
        if (null == tapRecordEvent) {
            return;
        }
        hasExactlyOnceWriteCache.set(handleExactlyOnceWriteCacheIfNeed(tapdataEvent, exactlyOnceWriteCache));
        List<String> lookupTables = initAndGetExactlyOnceWriteLookupList();
        String tgtTableNameFromTapEvent = getTgtTableNameFromTapEvent(tapRecordEvent);
        if (null != lookupTables && lookupTables.contains(tgtTableNameFromTapEvent) && hasExactlyOnceWriteCache.get() && eventExactlyOnceWriteCheckExists(tapdataEvent)) {
            if (obsLogger.isDebugEnabled()) {
                obsLogger.debug("Event check exactly once write exists, will ignore it: {}" + JSONUtil.obj2Json(tapRecordEvent));
            }
            return;
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
    }

    protected void handleTapdataAdjustMemoryEvent(TapdataAdjustMemoryEvent tapdataEvent) {
        try {
            int mode = tapdataEvent.getMode();
            double coefficient = tapdataEvent.getCoefficient();
            int newQueueSize = originalWriteQueueCapacity;
            switch (mode) {
                case TapdataAdjustMemoryEvent.INCREASE:
                    if (initialConcurrent && (null == this.initialPartitionConcurrentProcessor || !initialPartitionConcurrentProcessor.isRunning())) {
                        initTargetConcurrentProcessorIfNeed();
                        obsLogger.info("{}Target initial concurrent processor resumed", DynamicAdjustMemoryConstant.LOG_PREFIX);
                    }
                    newQueueSize = this.originalWriteQueueCapacity;
                    break;
                case TapdataAdjustMemoryEvent.DECREASE:
                    if (initialConcurrent && null != initialPartitionConcurrentProcessor && initialPartitionConcurrentProcessor.isRunning()) {
                        initialPartitionConcurrentProcessor.stop();
                        initialPartitionConcurrentProcessor = null;
                        obsLogger.info("{}Target initial concurrent processor stopped", DynamicAdjustMemoryConstant.LOG_PREFIX);
                    }
                    newQueueSize = BigDecimal.valueOf(this.originalWriteQueueCapacity).divide(BigDecimal.valueOf(coefficient).multiply(BigDecimal.valueOf(TARGET_QUEUE_FACTOR)), 0, RoundingMode.HALF_UP).intValue();
                    newQueueSize = Math.max(newQueueSize, 10);
                    newQueueSize = Math.min(newQueueSize, this.originalWriteQueueCapacity);
                    break;
                case TapdataAdjustMemoryEvent.KEEP:
                    break;
            }
            if (this.writeQueueCapacity != newQueueSize) {
                while (isRunning()) {
                    if (tapEventQueue.isEmpty() && tapEventProcessQueue.isEmpty()) {
                        this.tapEventQueue = new LinkedBlockingQueue<>(newQueueSize);
                        this.tapEventProcessQueue = new LinkedBlockingQueue<>(newQueueSize);
                        this.queueConsumerThreadPool.shutdownNow();
                        if (this.queueConsumerThreadPool.isShutdown()) {
                            ThreadGroup connectorOnTaskThreadGroup = Thread.currentThread().getThreadGroup();
                            queueConsumerThreadPool = AsyncUtils.createThreadPoolExecutor(String.format("Target-Queue-Consumer-%s[%s]@task-%s", getNode().getName(), getNode().getId(), dataProcessorContext.getTaskDto().getName()), 2, connectorOnTaskThreadGroup, TAG);
                            initTargetQueueConsumer();
                        }
                        obsLogger.info("{}Target queue size adjusted, old size: {}, new size: {}", DynamicAdjustMemoryConstant.LOG_PREFIX, this.writeQueueCapacity, newQueueSize);
                        this.writeQueueCapacity = newQueueSize;
                        break;
                    }
                    try {
                        TimeUnit.SECONDS.sleep(1L);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
            if (tapdataEvent.needAdjust()) {
                synchronized (this.dynamicAdjustQueueLock) {
                    this.dynamicAdjustQueueLock.notifyAll();
                    obsLogger.info("{}Notify target node to process data", DynamicAdjustMemoryConstant.LOG_PREFIX);
                }
            }
        } catch (Exception e) {
            synchronized (this.dynamicAdjustQueueLock) {
                this.dynamicAdjustQueueLock.notifyAll();
            }
            throw new TapCodeException(DynamicAdjustMemoryExCode_25.UNKNOWN_ERROR, e);
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
        flushOffsetByTapdataEventForNoConcurrent(new AtomicReference<>(tapdataEvent));
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
                    default:
                        break;
                }
            }
        }
    }

    private void flushShareCdcTableMetrics(TapdataEvent tapdataEvent) {
        if (tapdataEvent.getType().equals(SyncProgress.Type.LOG_COLLECTOR)) {
            Object connIdObj = tapdataEvent.getInfo(TapdataEvent.CONNECTION_ID_INFO_KEY);
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

    protected void handleTapdataShareLogEvent(List<TapdataShareLogEvent> tapdataShareLogEvents, TapdataEvent tapdataEvent, Consumer<TapdataEvent> consumer) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        if (tapEvent instanceof TapRecordEvent) {
            TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
            String targetTableName = getTgtTableNameFromTapEvent(tapEvent);
            replaceIllegalDateWithNullIfNeed(tapRecordEvent);
            fromTapValue(TapEventUtil.getBefore(tapRecordEvent), codecsFilterManager, targetTableName);
            fromTapValue(TapEventUtil.getAfter(tapRecordEvent), codecsFilterManager, targetTableName);
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
        syncMetricCollector.snapshotCompleted();
    }

    private void handleTapdataHeartbeatEvent(TapdataEvent tapdataEvent) {
        flushOffsetByTapdataEventForNoConcurrent(new AtomicReference<>(tapdataEvent));
    }

    protected TapRecordEvent handleTapdataRecordEvent(TapdataEvent tapdataEvent) {
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
        return tapRecordEvent;
    }

    protected void fromTapValueMergeInfo(TapdataEvent tapdataEvent) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        Object mergeInfoObj = tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY);
        if (mergeInfoObj instanceof MergeInfo) {
            MergeInfo mergeInfo = (MergeInfo) mergeInfoObj;
            List<MergeLookupResult> mergeLookupResults = mergeInfo.getMergeLookupResults();
            recursiveMergeInfoTransformFromTapValue(mergeLookupResults);
        }
    }

    protected void recursiveMergeInfoTransformFromTapValue(List<MergeLookupResult> mergeLookupResults) {
        if (CollectionUtils.isEmpty(mergeLookupResults)) return;
        for (MergeLookupResult mergeLookupResult : mergeLookupResults) {
            Map<String, Object> data = mergeLookupResult.getData();
            if (MapUtils.isNotEmpty(data)) {
                fromTapValue(data, codecsFilterManager, mergeLookupResult.getTapTable());
            }
            List<MergeLookupResult> childMergeLookupResults = mergeLookupResult.getMergeLookupResults();
            if (CollectionUtils.isNotEmpty(childMergeLookupResults)) {
                recursiveMergeInfoTransformFromTapValue(childMergeLookupResults);
            }
        }
    }

    protected void initIllegalDateAcceptable() {
        if (null == dataProcessorContext.getConnections()) {
            return;
        }
        List<Capability> capabilities = dataProcessorContext.getConnections().getCapabilities();
        if (CollectionUtils.isNotEmpty(capabilities) && capabilities.stream().anyMatch(cap -> null != cap && ConnectionOptions.DML_ILLEGAL_DATE_ACCEPTABLE.equals(cap.getId()))) {
            illegalDateAcceptable = true;
        }
    }

    protected void replaceIllegalDateWithNullIfNeed(TapRecordEvent event) {
        boolean containsIllegalDate = event.getContainsIllegalDate();
        if (containsIllegalDate && !illegalDateAcceptable) {
            Map<String, Object> before = Optional.ofNullable(TapEventUtil.getBefore(event)).orElse(new HashMap<>());
            replaceIllegalDate(before);
            Map<String, Object> after = Optional.ofNullable(TapEventUtil.getAfter(event)).orElse(new HashMap<>());
            replaceIllegalDate(after);
        }
    }

    protected void replaceIllegalDate(Map<String, Object> data) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (entry.getValue() instanceof TapDateTimeValue) {
                TapDateTimeValue tapDateTimeValue = (TapDateTimeValue) entry.getValue();
                replaceIllegalDateTime2Null(tapDateTimeValue.getValue(), entry);
            } else if (entry.getValue() instanceof TapDateValue) {
                TapDateValue tapDateValue = (TapDateValue) entry.getValue();
                replaceIllegalDateTime2Null(tapDateValue.getValue(), entry);
            } else if (entry.getValue() instanceof TapTimeValue) {
                TapTimeValue tapTimeValue = (TapTimeValue) entry.getValue();
                replaceIllegalDateTime2Null(tapTimeValue.getValue(), entry);
            } else if (entry.getValue() instanceof TapYearValue) {
                TapYearValue tapYearValue = (TapYearValue) entry.getValue();
                replaceIllegalDateTime2Null(tapYearValue.getValue(), entry);
            }
        }
    }

    protected void replaceIllegalDateTime2Null(DateTime tapDateValue, Map.Entry<String, Object> entry) {
        DateTime dateTime = tapDateValue;
        if (dateTime.isContainsIllegal()) {
            entry.setValue(null);
        }
    }


    protected boolean handleExactlyOnceWriteCacheIfNeed(TapdataEvent tapdataEvent, List<TapRecordEvent> exactlyOnceWriteCache) {
        if (!tableEnableExactlyOnceWrite(tapdataEvent.getSyncStage(), getTgtTableNameFromTapEvent(tapdataEvent.getTapEvent()))) {
            return false;
        }
        if (null == exactlyOnceWriteCache) {
            throw new IllegalArgumentException("Exactly once write cache list is null");
        }
        TapRecordEvent tapEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
        Long timestamp = TapEventUtil.getTimestamp(tapEvent);
        if (null == timestamp) {
            throw new TapCodeException(TapExactlyOnceWriteExCode_22.WRITE_CACHE_FAILED_TIMESTAMP_IS_NULL, String.format("Event from tableId:%s,exactlyOnceId is %s", tapEvent.getTableId(), tapEvent.getExactlyOnceId()))
                    .dynamicDescriptionParameters(tapEvent.getTableId(), tapEvent.getExactlyOnceId());
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
        Object updateMetadata = tapDDLEvent.getInfo(UPDATE_METADATA_INFO_KEY);
        if (updateMetadata instanceof Map && MapUtils.isNotEmpty((Map<?, ?>) updateMetadata)) {
            this.updateMetadata.putAll((Map<? extends String, ? extends MetadataInstancesDto>) updateMetadata);
        }
        if (tapDDLEvent instanceof TapCreateTableEvent) {
			TapCreateTableEvent createTableEvent = (TapCreateTableEvent) tapDDLEvent;
			boolean isSubPartitionTable = createTableEvent.getTable() != null &&
					createTableEvent.getTable().checkIsSubPartitionTable();
			if (isSubPartitionTable) {
				Object dagDataServiceObj = tapdataEvent.getTapEvent().getInfo(DAG_DATA_SERVICE_INFO_KEY);
				DAGDataServiceImpl dagDataService = null;
				if (dagDataServiceObj instanceof DAGDataServiceImpl) {
					dagDataService = (DAGDataServiceImpl) dagDataServiceObj;
				}
				if (dagDataService != null) {
					String partitionMasterTableId = createTableEvent.getPartitionMasterTableId() != null ?
							createTableEvent.getPartitionMasterTableId() : createTableEvent.getTable().getPartitionMasterTableId();
					MetadataInstancesDto metadata = dagDataService.getSchemaByNodeAndTableName(getNode().getId(), partitionMasterTableId);
					if (metadata != null && metadata.getId() != null) {
						this.updateMetadata.put(metadata.getId().toHexString(), metadata);
					}
				}
			}
		}
		Object insertMetadata = tapDDLEvent.getInfo(INSERT_METADATA_INFO_KEY);
		if (insertMetadata instanceof List && CollectionUtils.isNotEmpty((Collection<?>) insertMetadata)) {
			this.insertMetadata.addAll((Collection<? extends MetadataInstancesDto>) insertMetadata);
		}
		Object removeMetadata = tapDDLEvent.getInfo(REMOVE_METADATA_INFO_KEY);
		if (removeMetadata instanceof List && CollectionUtils.isNotEmpty((Collection<?>) removeMetadata)) {
			this.removeMetadata.addAll((Collection<? extends String>) removeMetadata);
		}
		if (tapDDLEvent instanceof TapCreateTableEvent && tapdataEvent.getBatchOffset() != null) {
			flushSyncProgressMap(tapdataEvent);
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
        SyncProgress syncProgress = this.syncProgressMap.computeIfAbsent(progressKey, k -> new SyncProgress());
        if (tapdataEvent instanceof TapdataStartingCdcEvent) {
            if (null == tapdataEvent.getSyncStage()) return;
            syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
        } else if (tapdataEvent instanceof TapdataHeartbeatEvent) {
            if (null == tapdataEvent.getSyncStage()) return;
            syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
            if (null != tapdataEvent.getStreamOffset()) {
                syncProgress.setStreamOffsetObj(tapdataEvent.getStreamOffset());
            }
            if (null != tapdataEvent.getBatchOffset()) {
                syncProgress.setBatchOffsetObj(tapdataEvent.getBatchOffset());
            }
            if (tapdataEvent.getSourceTime() != null)
				syncProgress.setSourceTime(tapdataEvent.getSourceTime());
			if (tapdataEvent.getSourceTime() != null)
				syncProgress.setEventTime(tapdataEvent.getSourceTime());
			flushOffset.set(true);
		} else if (tapdataEvent instanceof TapdataCompleteTableSnapshotEvent) {
			if (null != tapdataEvent.getBatchOffset() && syncProgress.getBatchOffsetObj() instanceof Map) {
				((Map<String, Object>) syncProgress.getBatchOffsetObj()).put(((TapdataCompleteTableSnapshotEvent) tapdataEvent).getSourceTableName(), tapdataEvent.getBatchOffset());
			}
		} else {
			if (null == tapdataEvent.getSyncStage()) return;
			if (null == tapdataEvent.getBatchOffset() && null == tapdataEvent.getStreamOffset()) return;
			if (SyncStage.CDC == tapdataEvent.getSyncStage() && null == tapdataEvent.getSourceTime()) return;
			if (null != tapdataEvent.getBatchOffset()) {
				if (tapdataEvent.getTapEvent() instanceof TapRecordEvent && syncProgress.getBatchOffsetObj() instanceof Map) {
					((Map<String, Object>) syncProgress.getBatchOffsetObj()).put(((TapRecordEvent) tapdataEvent.getTapEvent()).getTableId(), tapdataEvent.getBatchOffset());
				} else {
					syncProgress.setBatchOffsetObj(tapdataEvent.getBatchOffset());
				}
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
        everHandleTapTablePrimaryKeysMap.computeIfAbsent(tapTable.getId(), (value) -> {
            if (writeStrategy.equals(com.tapdata.tm.commons.task.dto.MergeTableProperties.MergeType.updateOrInsert.name())) {
                List<String> updateConditionFields = updateConditionFieldsMap.get(tapTable.getId());
                if (CollectionUtils.isNotEmpty(updateConditionFields)) {
                    Collection<String> pks = tapTable.primaryKeys();
                    if (!usePkAsUpdateConditions(updateConditionFields, pks)) {
                        ignorePksAndIndices(tapTable, updateConditionFields);
                    } else {
                        // 设置逻辑主键
                        tapTable.setLogicPrimaries(updateConditionFields);
                    }
                } else {
                    Collection<String> logicUniqueKey = tapTable.primaryKeys(true);
                    if (CollectionUtils.isEmpty(logicUniqueKey)) {
                        tapTable.setLogicPrimaries(NoPrimaryKeyVirtualField.getVirtualHashFieldNames(tapTable));
                    }
                }
            } else if (writeStrategy.equals(com.tapdata.tm.commons.task.dto.MergeTableProperties.MergeType.appendWrite.name())) {
                // 没有关联条件，清空主键信息
                ignorePksAndIndices(tapTable, null);
            }
            return true;
        });
    }

    protected static void ignorePksAndIndices(TapTable tapTable, List<String> logicPrimaries) {
        // fix: #140674 Bulk write data failed, write model list is empty, received record size: 7
        // The method may be called concurrently, need to clean the 'indexList' and field primaryKey mark after set 'logicPrimaries', because tapTable call the method 'primaryKey(true)' maybe empty
        tapTable.setLogicPrimaries(logicPrimaries);
        tapTable.setIndexList(null);
        tapTable.refreshPrimaryKeys();
        tapTable.getNameFieldMap().values().forEach(v -> {
            v.setPrimaryKeyPos(0);
            v.setPrimaryKey(false);
        });
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
                    if (syncProgress.getStreamOffset().length() > COMPRESS_STREAM_OFFSET_STRING_LENGTH_THRESHOLD) {
                        String compress = StringCompression.compress(syncProgress.getStreamOffset());
                        syncProgress.setStreamOffset(STREAM_OFFSET_COMPRESS_PREFIX + compress);
                    }
                }
                //System.out.println(JSONUtil.obj2JsonPretty(syncProgress.getBatchOffsetObj()));
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
//					TaskDto updateTaskDto = new TaskDto();
//					updateTaskDto.setId(taskDto.getId());
//					updateTaskDto.setDag(taskDto.getDag());
//
//
//					clientMongoOperator.insertOne(updateTaskDto, ConnectorConstant.TASK_COLLECTION + "/dag");
                    if (MapUtils.isNotEmpty(updateMetadata) || CollectionUtils.isNotEmpty(insertMetadata) || CollectionUtils.isNotEmpty(removeMetadata)) {
                        // Upload Metadata
                        TransformerWsMessageResult wsMessageResult = new TransformerWsMessageResult();
                        wsMessageResult.setBatchInsertMetaDataList(insertMetadata);
                        wsMessageResult.setBatchMetadataUpdateMap(updateMetadata);
                        wsMessageResult.setBatchRemoveMetaDataList(removeMetadata);
                        wsMessageResult.setTaskId(taskDto.getId().toHexString());
                        wsMessageResult.setTransformSchema(new HashMap<>());

                        if (taskDto.getDag() != null) {
                            List<Node> nodes = taskDto.getDag().getNodes();
                            if (CollectionUtils.isNotEmpty(nodes)) {
                                for (Node node : nodes) {
                                    node.setSchema(null);
                                    node.setOutputSchema(null);
                                }
                            }
                            wsMessageResult.setDag(taskDto.getDag());
                        }

                        String jsonResult = JsonUtil.toJsonUseJackson(wsMessageResult);
                        byte[] gzip = GZIPUtil.gzip(jsonResult.getBytes());
                        byte[] encode = Base64.getEncoder().encode(gzip);
                        String dataString = new String(encode, StandardCharsets.UTF_8);

                        // 返回结果调用接口返回
                        clientMongoOperator.insertOne(dataString, ConnectorConstant.TASK_COLLECTION + "/transformer/resultWithHistoryV2");
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

    protected PartitionConcurrentProcessor initCDCConcurrentProcessor(int concurrentWriteNum, Function<TapEvent, List<String>> partitionKeyFunction) {
        int batchSize = Math.max(this.targetBatch / concurrentWriteNum, DEFAULT_TARGET_BATCH) * 2;
        return new PartitionConcurrentProcessor(
                concurrentWriteNum,
                batchSize,
                new KeysPartitioner(),
                new TapEventPartitionKeySelector(partitionKeyFunction),
                this::handleTapdataEvents,
                this::flushSyncProgressMap,
                this::errorHandle,
                this::isRunning,
                dataProcessorContext.getTaskDto()
        );
    }

    protected PartitionConcurrentProcessor initInitialConcurrentProcessor(
            int concurrentWriteNum,
            Partitioner<TapdataEvent, List<Object>> partitioner
    ) {
        int batchSize = Math.max(this.targetBatch / concurrentWriteNum, DEFAULT_TARGET_BATCH) * 2;
        PartitionConcurrentProcessor partitionConcurrentProcessor = new PartitionConcurrentProcessor(
                concurrentWriteNum,
                batchSize,
                partitioner,
                new PartitionKeySelector<TapEvent, Object, Map<String, Object>>() {
                    @Override
                    public List<Object> select(TapEvent event, Map<String, Object> row) {
                        return Collections.emptyList();
                    }

                    @Override
                    public List<Object> convert2OriginValue(List<Object> values) {
                        return Collections.emptyList();
                    }
                },
                this::handleTapdataEvents,
                this::flushSyncProgressMap,
                this::errorHandle,
                this::isRunning,
                dataProcessorContext.getTaskDto()
        );
        return partitionConcurrentProcessor;
    }

    @Override
    public void doClose() throws TapCodeException {
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
            CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.dynamicAdjustQueueLock).ifPresent(l -> {
                synchronized (l) {
                    l.notifyAll();
                }
            }), TAG);
            CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.flushOffsetExecutor).ifPresent(ExecutorService::shutdownNow), TAG);
            CommonUtils.ignoreAnyError(this::saveToSnapshot, TAG);
            CommonUtils.ignoreAnyError(() -> syncMetricCollector.close(obsLogger), TAG);
        } finally {
            super.doClose();
        }
    }

    protected boolean usePkAsUpdateConditions(Collection<String> updateConditions, Collection<String> pks) {
        if (pks == null) {
            pks = Collections.emptySet();
        }
        if (pks.size() != updateConditions.size()) {
            return false;
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

    protected boolean tableEnableExactlyOnceWrite(SyncStage syncStage, String tableId) {
        return SyncStage.CDC.equals(syncStage) && exactlyOnceWriteTables.contains(tableId);
    }

    protected List<String> initAndGetExactlyOnceWriteLookupList() {
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

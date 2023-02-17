package io.tapdata.flow.engine.V2.node.hazelcast;

import cn.hutool.core.date.StopWatch;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.constant.DataFlowUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.Job;
import com.tapdata.entity.JoinTable;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.RuntimeInfo;
import com.tapdata.entity.Stats;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.DataFlowSetting;
import com.tapdata.entity.dataflow.RuntimeThroughput;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.DataFunctionAspect;
import io.tapdata.aspect.DataNodeCloseAspect;
import io.tapdata.aspect.DataNodeInitAspect;
import io.tapdata.aspect.ProcessorNodeCloseAspect;
import io.tapdata.aspect.ProcessorNodeInitAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.SettingService;
import io.tapdata.entity.OnData;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.flow.engine.V2.common.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.monitor.impl.JetJobStatusMonitor;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNode;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation.HazelcastMultiAggregatorProcessor;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskClient;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.NodeUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.milestone.MilestoneContext;
import io.tapdata.milestone.MilestoneFactory;
import io.tapdata.milestone.MilestoneService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author jackin
 * @date 2021/12/7 3:25 PM
 **/
public abstract class HazelcastBaseNode extends AbstractProcessor {
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkDataNode.class);
	/**
	 * [sub task id]-[node id]
	 */
	private final static String THREAD_NAME_TEMPLATE = "[%s-%s]";
	protected static final String NEW_DAG_INFO_KEY = "NEW_DAG";
	protected static final String DAG_DATA_SERVICE_INFO_KEY = "DAG_DATA_SERVICE";
	protected static final String TRANSFORM_SCHEMA_ERROR_MESSAGE_INFO_KEY = "TRANSFORM_SCHEMA_ERROR_MESSAGE";
	protected static final String UPDATE_METADATA_INFO_KEY = "UPDATE_METADATA";
	protected static final String INSERT_METADATA_INFO_KEY = "INSERT_METADATA";
	protected static final String REMOVE_METADATA_INFO_KEY = "REMOVE_METADATA";
	protected static final String QUALIFIED_NAME_ID_MAP_INFO_KEY = "QUALIFIED_NAME_ID_MAP";
	private static final String TAG = HazelcastBaseNode.class.getSimpleName();

	//  protected BaseMetrics taskNodeMetrics;
//  protected ScheduledExecutorService metricsThreadPool;
//  protected ScheduledFuture<?> metricsThreadPoolFuture;
	protected ClientMongoOperator clientMongoOperator;
	protected Context jetContext;
	protected SettingService settingService;

	protected Map<String, String> tags;
	protected MilestoneService milestoneService;
	protected NodeException error;
	protected String errorMessage;
	protected ProcessorBaseContext processorBaseContext;
	protected String threadName;

	public AtomicBoolean running = new AtomicBoolean(false);
	protected TapCodecsFilterManager codecsFilterManager;

	/**
	 * Whether to process data from multiple tables
	 */
	protected final boolean multipleTables;

	protected ObsLogger obsLogger;
	protected MonitorManager monitorManager;
	private JetJobStatusMonitor jetJobStatusMonitor;

	public HazelcastBaseNode(ProcessorBaseContext processorBaseContext) {
//		isJobRunning = new TapCache<Boolean>().expireTime(2000).supplier(this::isJetJobRunning).disableCacheValue(false);

		this.processorBaseContext = processorBaseContext;

		this.obsLogger = ObsLoggerFactory.getInstance().getObsLogger(
				processorBaseContext.getTaskDto(),
				processorBaseContext.getNode().getId(),
				processorBaseContext.getNode().getName()
		);

		if (null != processorBaseContext.getConfigurationCenter()) {
			this.clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);

			this.settingService = new SettingService(clientMongoOperator);
		}
		if (null != processorBaseContext.getNode() && null == processorBaseContext.getNode().getGraph()) {
			Dag dag = new Dag(processorBaseContext.getEdges(), processorBaseContext.getNodes());
			DAG _DAG = DAG.build(dag);
			_DAG.setTaskId(processorBaseContext.getTaskDto().getId());
			processorBaseContext.getTaskDto().setDag(_DAG);
		}

		threadName = String.format(THREAD_NAME_TEMPLATE, processorBaseContext.getTaskDto().getId().toHexString(), processorBaseContext.getNode() != null ? processorBaseContext.getNode().getName() : null);

		//如果为迁移任务、且源节点为数据库类型
		this.multipleTables = CollectionUtils.isNotEmpty(processorBaseContext.getTaskDto().getDag().getSourceNode());
		if (!StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			this.monitorManager = new MonitorManager();
		}
	}

	public <T extends DataFunctionAspect<T>> AspectInterceptResult executeDataFuncAspect(Class<T> aspectClass, Callable<T> aspectCallable, CommonUtils.AnyErrorConsumer<T> anyErrorConsumer) {
		return AspectUtils.executeDataFuncAspect(aspectClass, aspectCallable, anyErrorConsumer);
	}

	public <T extends Aspect> AspectInterceptResult executeAspect(Class<T> aspectClass, Callable<T> aspectCallable) {
		return AspectUtils.executeAspect(aspectClass, aspectCallable);
	}

	public <T extends Aspect> AspectInterceptResult executeAspect(T aspect) {
		return AspectUtils.executeAspect(aspect);
	}

	protected void doInit(@NotNull Processor.Context context) throws Exception {
	}

	@Override
	public final void init(@NotNull Processor.Context context) throws Exception {
		try {
			this.jetContext = context;
			super.init(context);
			Log4jUtil.setThreadContext(processorBaseContext.getTaskDto());
			running.compareAndSet(false, true);
			TapCodecsRegistry tapCodecsRegistry = TapCodecsRegistry.create();
			tapCodecsRegistry.registerFromTapValue(TapDateTimeValue.class, tapValue -> tapValue.getValue().toInstant());
			codecsFilterManager = TapCodecsFilterManager.create(tapCodecsRegistry);
			// execute ProcessorNodeInitAspect before doInit since we need to init the aspect first;
			if (this instanceof HazelcastProcessorBaseNode || this instanceof HazelcastMultiAggregatorProcessor) {
				AspectUtils.executeAspect(ProcessorNodeInitAspect.class, () -> new ProcessorNodeInitAspect().processorBaseContext(processorBaseContext));
			} else {
				AspectUtils.executeAspect(DataNodeInitAspect.class, () -> new DataNodeInitAspect().dataProcessorContext((DataProcessorContext) processorBaseContext));
			}
			monitorManager.startMonitor(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR, context.hazelcastInstance().getJet().getJob(context.jobId()), processorBaseContext.getNode().getId());
			jetJobStatusMonitor = (JetJobStatusMonitor) monitorManager.getMonitorByType(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR);
			doInit(context);
		} catch (Throwable e) {
			errorHandle(e, "Node init failed: " + e.getMessage());
		}
	}

	public ProcessorBaseContext getProcessorBaseContext() {
		return processorBaseContext;
	}

	protected Job convert2Job(DataFlow dataFlow, Node node, ClientMongoOperator clientMongoOperator) {
		final List<Job> jobs = DataFlowUtil.convertDataFlowToJobs(dataFlow, clientMongoOperator);
		if (CollectionUtils.isNotEmpty(jobs)) {
			for (Job job : jobs) {
				final RuntimeInfo runtimeInfo = new RuntimeInfo();
				runtimeInfo.setUnSupportedDDLS(new ArrayList<>());
				job.setRuntimeInfo(runtimeInfo);
				final List<Stage> stages = job.getStages();
				if (CollectionUtils.isNotEmpty(stages)) {
					if (stages.stream().anyMatch(s -> s.getId().equals(node.getId()))) {
						job.setId(node.getId());
						job.setStatus(ConnectorConstant.SCHEDULED);
						job.setStatus(ConnectorConstant.RUNNING);
						job.setTaskId(dataFlow.getTaskId());
						job.setSubTaskId(dataFlow.getSubTaskId());
						job.setClientMongoOperator(clientMongoOperator);
						return job;
					}
				}
			}
		}

		return null;
	}

	protected TapValueTransform transformFromTapValue(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent.getTapEvent()) return null;
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		TapValueTransform tapValueTransform = TapValueTransform.create();
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		if (MapUtils.isNotEmpty(before)) {
			tapValueTransform.before(codecsFilterManager.transformFromTapValueMap(before));
		}
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (MapUtils.isNotEmpty(after)) {
			tapValueTransform.after(codecsFilterManager.transformFromTapValueMap(after));
		}
		return tapValueTransform;
	}

	protected void transformToTapValue(TapdataEvent tapdataEvent, TapTableMap<String, TapTable> tapTableMap, String tableName) {
		transformToTapValue(tapdataEvent, tapTableMap, tableName, null);
	}

	protected void transformToTapValue(TapdataEvent tapdataEvent, TapTableMap<String, TapTable> tapTableMap, String tableName, TapValueTransform tapValueTransform) {
		if (!(tapdataEvent.getTapEvent() instanceof TapRecordEvent)) return;
		if (null == tapTableMap)
			throw new IllegalArgumentException("Transform to TapValue failed, tapTableMap is empty, table name: " + tableName);
		TapTable tapTable = tapTableMap.get(tableName);
		if (null == tapTable)
			throw new IllegalArgumentException("Transform to TapValue failed, table schema is empty, table name: " + tableName);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (nameFieldMap == null)
			throw new IllegalArgumentException("Transform to TapValue failed, field map is empty, table name: " + tableName);
		TapEvent tapEvent = tapdataEvent.getTapEvent();

		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (null != tapValueTransform) {
			if (MapUtils.isNotEmpty(before))
				codecsFilterManager.transformToTapValueMap(before, nameFieldMap, tapValueTransform.getBefore());
			if (MapUtils.isNotEmpty(after))
				codecsFilterManager.transformToTapValueMap(after, nameFieldMap, tapValueTransform.getAfter());
		} else {
			if (MapUtils.isNotEmpty(before)) codecsFilterManager.transformToTapValueMap(before, nameFieldMap);
			if (MapUtils.isNotEmpty(after)) codecsFilterManager.transformToTapValueMap(after, nameFieldMap);
		}
	}

	protected MessageEntity tapEvent2Message(TapRecordEvent dataEvent) {
		MessageEntity messageEntity = new MessageEntity();
		Map<String, Object> before = TapEventUtil.getBefore(dataEvent);
		messageEntity.setBefore(before);
		Map<String, Object> after = TapEventUtil.getAfter(dataEvent);
		messageEntity.setAfter(after);
		messageEntity.setOp(TapEventUtil.getOp(dataEvent));
		messageEntity.setTableName(dataEvent.getTableId());
		messageEntity.setTimestamp(dataEvent.getReferenceTime());
		messageEntity.setTime(dataEvent.getTime());
		return messageEntity;
	}

	protected TapRecordEvent message2TapEvent(MessageEntity messageEntity) {
		TapRecordEvent tapRecordEvent;
		String op = messageEntity.getOp();
		OperationType operationType = OperationType.fromOp(op);
		if (operationType == null) {
			return null;
		}
		switch (operationType) {
			case INSERT:
				tapRecordEvent = new TapInsertRecordEvent();
				((TapInsertRecordEvent) tapRecordEvent).setAfter(messageEntity.getAfter());
				break;
			case UPDATE:
				tapRecordEvent = new TapUpdateRecordEvent();
				((TapUpdateRecordEvent) tapRecordEvent).setBefore(messageEntity.getBefore());
				((TapUpdateRecordEvent) tapRecordEvent).setAfter(messageEntity.getAfter());
				break;
			case DELETE:
				tapRecordEvent = new TapDeleteRecordEvent();
				((TapDeleteRecordEvent) tapRecordEvent).setBefore(messageEntity.getBefore());
				break;
			default:
				tapRecordEvent = null;
				break;
		}
		if (null != tapRecordEvent) {
			tapRecordEvent.setTableId(messageEntity.getTableName());
			tapRecordEvent.setReferenceTime(messageEntity.getTimestamp());
			tapRecordEvent.setTime(messageEntity.getTime());
		}
		return tapRecordEvent;
	}

	protected String getTableName(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) return "";
		if (null != tapdataEvent.getMessageEntity()) {
			return tapdataEvent.getMessageEntity().getTableName();
		} else {
			TapEvent tapEvent = tapdataEvent.getTapEvent();
			if (tapEvent instanceof TapRecordEvent) {
				return ((TapRecordEvent) tapEvent).getTableId();
			} else {
				return "";
			}
		}
	}

	private int bucketIndex = 0;

	protected boolean offer(TapdataEvent dataEvent) {

		if (dataEvent != null) {
			if (processorBaseContext.getNode() != null) {
				dataEvent.addNodeId(processorBaseContext.getNode().getId());
			}
			Outbox outbox = getOutbox();
			if (null != outbox) {
				final int bucketCount = outbox.bucketCount();
				if (bucketCount > 1) {
					for (bucketIndex = Math.min(bucketIndex, bucketCount); bucketIndex < bucketCount; bucketIndex++) {
						final TapdataEvent cloneEvent = (TapdataEvent) dataEvent.clone();
						if (!tryEmit(bucketIndex, cloneEvent)) {
							return false;
						}
					}
				} else if (!tryEmit(dataEvent)) {
					return false;
				}
			}
		}
		bucketIndex = 0; // reset to 0 of return true
		return true;
	}

	protected DataFlow convertTask2DataFlow(ProcessorBaseContext processorBaseContext) {
		TaskDto taskDto = processorBaseContext.getTaskDto();
		Node<?> node = processorBaseContext.getNode();
		List<Node> nodes = processorBaseContext.getNodes();
		List<Edge> edges = processorBaseContext.getEdges();
		DataFlow dataFlow = new DataFlow();
		dataFlow.setId(taskDto.getId().toHexString());
		dataFlow.setName(taskDto.getName());
		dataFlow.setStatus(taskDto.getStatus());
		dataFlow.setTaskId(taskDto.getId().toHexString());
		dataFlow.setSubTaskId(taskDto.getId().toHexString());
		dataFlow.setUser_id(taskDto.getUserId());
		if (node instanceof DatabaseNode) {
			dataFlow.setMappingTemplate(ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE);
		} else {
			dataFlow.setMappingTemplate(ConnectorConstant.MAPPING_TEMPLATE_CUSTOM);
		}

		DataFlowSetting setting = new DataFlowSetting();
		setting.setCdcConcurrency(taskDto.getIncreSyncConcurrency());
		setting.setIsOpenAutoDDL(taskDto.getIsOpenAutoDDL());
		setting.setIsSchedule(taskDto.getIsSchedule());
		setting.setStopOnError(taskDto.getIsStopOnError());
		setting.setTransformerConcurrency(8);
		setting.setReadCdcInterval(500);

		setting.setCdcFetchSize(taskDto.getIncreaseReadSize());
		setting.setCdcShareFilterOnServer(!taskDto.getIsFilter());
		setting.setCronExpression(taskDto.getCrontabExpression());
		setting.setDistinctWriteType("force".equals(taskDto.getDeduplicWriteMode()) ? ConnectorConstant.DISTINCT_WRITE_TYPE_COMPEL : ConnectorConstant.DISTINCT_WRITE_TYPE_INTELLECT);

		// For data write idempotent, used force de-duplicate mode when source has join node
		final List<Node> allPreNodes = NodeUtil.findAllPreNodes(node);
		if (CollectionUtils.isNotEmpty(allPreNodes)) {
			final boolean hasJoinNode = allPreNodes.stream().anyMatch(n -> NodeTypeEnum.JOIN.type.equals(n.getType()));
			if (hasJoinNode) {
				setting.setDistinctWriteType(ConnectorConstant.DISTINCT_WRITE_TYPE_COMPEL);
			}
		}
		if (node instanceof TableNode) {
			setting.setMaxTransactionLength(Double.valueOf(((TableNode) node).getMaxTransactionDuration()));
		}
		setting.setNeedToCreateIndex(taskDto.getIsAutoCreateIndex());
		//todo
		setting.setProcessorConcurrency(1);
		setting.setReadBatchSize(100);
		setting.setSync_type(taskDto.getType());
		dataFlow.setSetting(setting);
		setting.setNoPrimaryKey(true);

		if (CollectionUtils.isNotEmpty(nodes)) {
			List<Stage> stages = new ArrayList<>();
			Map<String, Stage> stageMap = new HashMap<>();
			for (Node tmpNode : nodes) {
				final Stage stage = HazelcastUtil.node2CommonStage(tmpNode);
				if (stage != null) {
					stageMap.put(tmpNode.getId(), stage);
					stages.add(stage);
				}
			}

			if (CollectionUtils.isNotEmpty(edges)) {
				// Set input/output lanes
				for (Edge edge : edges) {
					String source = edge.getSource();
					String target = edge.getTarget();
					Stage srcStage = stageMap.get(source);
					srcStage.getOutputLanes().add(target);
					Stage tgtStage = stageMap.get(target);
					tgtStage.getInputLanes().add(source);
				}
				// Set join tables
				for (Edge edge : edges) {
					final String target = edge.getTarget();
					final Stage tgtStage = stageMap.get(target);
					if (DataFlowStageUtil.isDataStage(tgtStage.getType())) {
						if (ConnectorConstant.MAPPING_TEMPLATE_CUSTOM.equals(dataFlow.getMappingTemplate())) {
							final List<String> inputLanes = tgtStage.getInputLanes();
							for (String inputLane : inputLanes) {
								final Set<String> sourceDataStages = DataFlowUtil.findSourceDataStagesByInputLane(stages, inputLane);
								for (String sourceDataStage : sourceDataStages) {
									final Stage stage = stageMap.get(sourceDataStage);
									JoinTable joinTable = getJoinTable(node);
									if (joinTable != null) {
										joinTable.setStageId(stage.getId());
										if (CollectionUtils.isEmpty(tgtStage.getJoinTables())) {
											tgtStage.setJoinTables(new ArrayList<>());
										}
										tgtStage.getJoinTables().add(joinTable);
									}
								}
							}
						}
					}
				}
			}

			final List<Stage> reachableStage = DataFlowStageUtil.findReachableStageEndByDataStage(node.getId(), stages);
			if (reachableStage != null) {
				stages = stages.stream().filter(s -> reachableStage.contains(s) || s.getId().equals(node.getId())).collect(Collectors.toCollection(ArrayList::new));
			}
			dataFlow.setStages(stages);
		}

		return dataFlow;
	}

	private JoinTable getJoinTable(Node node) {
		JoinTable joinTable = null;
		if (node instanceof TableNode) {
			TableNode tableNode = (TableNode) node;
			final List<String> updateConditionFields = tableNode.getUpdateConditionFields();
			List<Map<String, String>> joinKeys = new ArrayList<>();
			if (CollectionUtils.isNotEmpty(updateConditionFields)) {
				for (String updateConditionField : updateConditionFields) {
					joinKeys.add(new HashMap<String, String>() {{
						put("source", updateConditionField);
						put("target", updateConditionField);
					}});
				}
			}
			joinTable = new JoinTable();
			joinTable.setJoinKeys(joinKeys);
			joinTable.setJoinType("upsert");
		}

		// todo
//    switch (tableNode.getWriteStrategy()) {
//      case "updateOrInsert":
//        joinTable.setJoinType("upsert");
//        break;
//      case "appendWrite":
//        joinTable.setJoinType("append");
//        break;
//      case "updateWrite":
//        joinTable.setJoinType("update");
//        break;
//    }
		return joinTable;
	}

	protected void doClose() throws Exception {
		CommonUtils.handleAnyError(() -> {
			Optional.ofNullable(processorBaseContext.getTapTableMap()).ifPresent(TapTableMap::reset);
			logger.info(String.format("Node %s[%s] schema data cleaned", getNode().getName(), getNode().getId()));
			obsLogger.info(String.format("Node %s[%s] schema data cleaned", getNode().getName(), getNode().getId()));
		}, err -> {
			logger.warn(String.format("Clean node %s[%s] schema data failed: %s", getNode().getName(), getNode().getId(), err.getMessage()));
			obsLogger.warn(String.format("Clean node %s[%s] schema data failed: %s", getNode().getName(), getNode().getId(), err.getMessage()));
		});
	}

	@Override
	public final void close() throws Exception {
		StopWatch sw = new StopWatch();
		try {
			sw.start();
			running.set(false);
			obsLogger.info(String.format("Node %s[%s] running status set to false", getNode().getName(), getNode().getId()));
			CommonUtils.handleAnyError(this::doClose, err -> {
				obsLogger.warn(String.format("Close node failed: %s | Node: %s[%s] | Type: %s", err.getMessage(), getNode().getName(), getNode().getId(), this.getClass().getName()));
			});
			CommonUtils.ignoreAnyError(() -> {
				if (this instanceof HazelcastProcessorBaseNode || this instanceof HazelcastMultiAggregatorProcessor) {
					AspectUtils.executeAspect(ProcessorNodeCloseAspect.class, () -> new ProcessorNodeCloseAspect().processorBaseContext(processorBaseContext));
				} else {
					AspectUtils.executeAspect(DataNodeCloseAspect.class, () -> new DataNodeCloseAspect().dataProcessorContext((DataProcessorContext) processorBaseContext));
				}
			}, TAG);
		} finally {
			ThreadContext.clearAll();
			super.close();
			sw.stop();
			obsLogger.info(String.format("Node %s[%s] close complete, cost %d ms", getNode().getName(), getNode().getId(), sw.getTotalTimeMillis()));
		}
	}

	public void setMilestoneService(MilestoneService milestoneService) {
		this.milestoneService = milestoneService;
	}

	protected void onDataStats(OnData onData, Stats stats) {
		Map<String, Long> total = stats.getTotal();
		Long processed = total.getOrDefault(Stats.PROCESSED_FIELD_NAME, 0L);
		Long sourceReceived = total.getOrDefault(Stats.SOURCE_RECEIVED_FIELD_NAME, 0L);
		Long targetInserted = total.getOrDefault(Stats.TARGET_INSERTED_FIELD_NAME, 0L);
		Long totalUpdated = total.getOrDefault(Stats.TOTAL_UPDATED_FIELD_NAME, 0L);
		Long totalDeleted = total.getOrDefault(Stats.TOTAL_DELETED_FIELD_NAME, 0L);
		Long fileSize = total.getOrDefault(Stats.TOTAL_FILE_LENGTH_FIELD_NAME, 0L);
		Long totalDataQuality = total.getOrDefault(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME, 0L);

		processed += onData.getProcessed();
		sourceReceived += onData.getSource_received();
		targetInserted += onData.getTarget_inserted();
		totalUpdated += onData.getTotal_updated();
		totalDeleted += onData.getTotal_deleted();
		fileSize += onData.getTotal_file_length();
		if (onData.getTotal_data_quality() <= onData.getProcessed()) {
			totalDataQuality += onData.getTotal_data_quality();
		}
		totalDataQuality = totalDataQuality > processed ? processed : totalDataQuality;

		total.put(Stats.PROCESSED_FIELD_NAME, processed);
		total.put(Stats.SOURCE_RECEIVED_FIELD_NAME, sourceReceived);
		total.put(Stats.TARGET_INSERTED_FIELD_NAME, targetInserted);
		total.put(Stats.TOTAL_UPDATED_FIELD_NAME, totalUpdated);
		total.put(Stats.TOTAL_DELETED_FIELD_NAME, totalDeleted);
		total.put(Stats.TOTAL_FILE_LENGTH_FIELD_NAME, fileSize);
		total.put(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME, totalDataQuality);

		List<StageRuntimeStats> stageRuntimeStats = stats.getStageRuntimeStats();

    /*Map<String, List<String>> inputMap = new HashMap<>();
    for (Stage stage : context.getJob().getStages()) {
      if (stage.getInputLanes().size() != 0) {
        inputMap.put(stage.getId(), stage.getInputLanes());
      }
    }*/

		for (StageRuntimeStats stageRuntimeStat : stageRuntimeStats) {
			String stageId = stageRuntimeStat.getStageId();
			if (onData.getInsertStage().containsKey(stageId)) {
				RuntimeThroughput runtimeThroughput = onData.getInsertStage().get(stageId);
				stageRuntimeStat.incrementInsert(runtimeThroughput);
			}
			if (onData.getUpdateStage().containsKey(stageId)) {
				RuntimeThroughput runtimeThroughput = onData.getUpdateStage().get(stageId);
				stageRuntimeStat.incrementUpdate(runtimeThroughput);
			}
			if (onData.getDeleteStage().containsKey(stageId)) {
				RuntimeThroughput runtimeThroughput = onData.getDeleteStage().get(stageId);
				stageRuntimeStat.incrementDelete(runtimeThroughput);
			}
		}

		// metrics.updateMetrics(onData);

    /*List<MessageEntity> msgs = onData.getMsgs();
    List<InitialStat> initialStats = context.getTapdataShareContext().getInitialStats();
    if (CollectionUtils.isNotEmpty(msgs) && initialStats != null) {
      Map<String, Long> map = new HashMap<>();
      for (MessageEntity msg : msgs) {
        if (msg.getMapping() == null || !Objects.equals(OperationType.fromOp(msg.getOp()).getType(), "dml")) {
          continue;
        }

        String key = context.getJobSourceConn().getId() + "_" + msg.getMapping().getFrom_table() + "_" + context.getJobTargetConn().getId() + "_" + msg.getMapping().getTo_table();

        if (map.containsKey(key)) {
          Long value = map.get(key);
          map.put(key, ++value);
        } else {
          map.put(key, 1L);
        }
      }

      // let gc work
      onData.setMsgs(null);

      for (Map.Entry<String, Long> entry : map.entrySet()) {
        String key = entry.getKey();
        Long count = entry.getValue();

        initialStats.stream().filter(countStat -> (countStat.getSourceConnectionId() + "_" + countStat.getSourceTableName() + "_" + countStat.getTargetConnectionId() + "_" + countStat.getTargetTableName()).equals(key))
          .findFirst().ifPresent(cs -> cs.setTargetRowNum(cs.getTargetRowNum() + count));
      }
    }*/
	}

	public Node getNode() {
		return processorBaseContext.getNode();
	}

	protected void initMilestoneService(MilestoneContext.VertexType vertexType) {
		Node<?> node = processorBaseContext.getNode();
		String vertexName = NodeUtil.getVertexName(node);
		List<Node<?>> nextOrPreDataNodes;
		switch (vertexType) {
			case SOURCE:
				nextOrPreDataNodes = GraphUtil.successors(node, Node::isDataNode);
				break;
			case DEST:
				nextOrPreDataNodes = GraphUtil.predecessors(node, Node::isDataNode);
				break;
			default:
				nextOrPreDataNodes = new ArrayList<>();
				break;
		}

		List<String> vertexNames = nextOrPreDataNodes.stream().map(NodeUtil::getVertexName).collect(Collectors.toList());

		HttpClientMongoOperator httpClientMongoOperator = (HttpClientMongoOperator) clientMongoOperator;
		this.milestoneService = MilestoneFactory.getJetEdgeMilestoneService(processorBaseContext.getTaskDto(), httpClientMongoOperator.getRestTemplateOperator().getBaseURLs(), httpClientMongoOperator.getRestTemplateOperator().getRetryTime(), httpClientMongoOperator.getConfigCenter(), node, vertexName, vertexNames, null, vertexType);
	}

	protected synchronized NodeException errorHandle(Throwable throwable, String errorMessage) {
		NodeException currentEx;
		if (throwable instanceof NodeException) {
			currentEx = (NodeException) throwable;
		} else {
			currentEx = new NodeException(errorMessage, throwable).context(getProcessorBaseContext());
		}
		try {
			if (null == error) {
				this.error = currentEx;
				if (null != errorMessage) {
					this.errorMessage = errorMessage;
				} else {
					this.errorMessage = currentEx.getMessage();
				}
				logger.error(errorMessage, currentEx);
				obsLogger.error(errorMessage, currentEx);
				this.running.set(false);
				TaskDto taskDto = processorBaseContext.getTaskDto();

				// jetContext async injection, Attempt 5 times to get the instance every 500ms
				com.hazelcast.jet.Job hazelcastJob = null;
				for (int i = 5; i > 0; i--) {
					if (null != jetContext) {
						hazelcastJob = jetContext.hazelcastInstance().getJet().getJob(taskDto.getName() + "-" + taskDto.getId().toHexString());
					}

					if (null != hazelcastJob) break;
					try {
						Thread.sleep(500);
					} catch (InterruptedException ignored) {
						break;
					}
				}

				if (hazelcastJob != null) {
					JobStatus status = hazelcastJob.getStatus();
					if (JobStatus.COMPLETING != status) {
						logger.info("Job cancel in error handle");
						obsLogger.info("Job cancel in error handle");
						TaskClient<TaskDto> taskDtoTaskClient = BeanUtil.getBean(TapdataTaskScheduler.class).getTaskClientMap().get(taskDto.getId().toHexString());
						if (null != taskDtoTaskClient) {
							taskDtoTaskClient.terminalMode(TerminalMode.ERROR);
							taskDtoTaskClient.error(error);
						}
						hazelcastJob.suspend();
					}
				} else {
					logger.warn("The jet instance cannot be found and needs to be stopped manually", currentEx);
					obsLogger.warn("The jet instance cannot be found and needs to be stopped manually", currentEx);
				}
			}
		} catch (Exception e) {
			logger.warn("Error handler failed: " + e.getMessage(), e);
			obsLogger.warn("Error handler failed: " + e.getMessage(), e);
		}

		return currentEx;
	}

	protected boolean taskHasBeenRun() {
		final TaskDto taskDto = processorBaseContext.getTaskDto();
		if (taskDto != null && MapUtils.isNotEmpty(taskDto.getAttrs())) {
			return taskDto.getAttrs().containsKey("syncProgress");
		}

		return false;
	}

	@Override
	public boolean isCooperative() {
		return false;
	}

	protected boolean isRunning() {
		//isJetJobRunning has thread lock and not a simple implementation.
		//Should avoid invoke isJetJobRunning method for every event.
		//Use TapCache to cache the isJetJobRunning's result, expire in 2 seconds.
		//Then no more performance issue.
		return running.get() && !Thread.currentThread().isInterrupted() && isJetJobRunning();
	}


	protected void updateMemoryFromDDLInfoMap(TapdataEvent tapdataEvent, String tableName) {
		if (null == tapdataEvent) {
			return;
		}
		if (!tapdataEvent.isDDL()) {
			return;
		}
		try {
			updateDAG(tapdataEvent);
		} catch (Exception e) {
			throw new RuntimeException("Update memory DAG failed, error: " + e.getMessage(), e);
		}
		try {
			updateNode(tapdataEvent);
		} catch (Exception e) {
			throw new RuntimeException("Update memory node failed, error: " + e.getMessage(), e);
		}
		try {
			updateTapTable(tapdataEvent, tableName);
		} catch (Exception e) {
			throw new RuntimeException("Update memory TapTable failed, error: " + e.getMessage(), e);
		}
	}

	protected void updateDAG(TapdataEvent tapdataEvent) {
		Object newDAG = tapdataEvent.getTapEvent().getInfo(NEW_DAG_INFO_KEY);
		if (!(newDAG instanceof DAG)) {
			return;
		}
		processorBaseContext.getTaskDto().setDag((DAG) newDAG);
		processorBaseContext.setNodes(((DAG) newDAG).getNodes());
		processorBaseContext.setEdges(((DAG) newDAG).getEdges());
	}

	protected void updateNode(TapdataEvent tapdataEvent) {
		Object newDAG = tapdataEvent.getTapEvent().getInfo(NEW_DAG_INFO_KEY);
		if (!(newDAG instanceof DAG)) {
			return;
		}
		String nodeId = getNode().getId();
		Node<?> newNode = ((DAG) newDAG).getNode(nodeId);
		processorBaseContext.setNode(newNode);
		updateNodeConfig();
	}

	protected void updateTapTable(TapdataEvent tapdataEvent) {
		updateTapTable(tapdataEvent, null);
	}

	protected void updateTapTable(TapdataEvent tapdataEvent, String tableName) {
		Object dagDataService = tapdataEvent.getTapEvent().getInfo(DAG_DATA_SERVICE_INFO_KEY);
		if (!(dagDataService instanceof DAGDataServiceImpl)) {
			return;
		}
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
		if (StringUtils.isBlank(tableName)) {
			tableName = getNode().getId();
		}
		String qualifiedName = tapTableMap.getQualifiedName(tableName);
		if (tapEvent instanceof TapCreateTableEvent) {
			Object insertMetadata = tapEvent.getInfo(INSERT_METADATA_INFO_KEY);
			if (insertMetadata instanceof List) {
//				String finalTableName = tableName;
//				MetadataInstancesDto metadata = ((DAGDataServiceImpl) dagDataService).getBatchInsertMetaDataList().stream().filter(m -> m.getOriginalName().equals(finalTableName))
//						.findFirst().orElse(null);
				MetadataInstancesDto metadata = ((DAGDataServiceImpl) dagDataService).getSchemaByNodeAndTableName(getNode().getId(), tableName);
				if (null != metadata) {
					qualifiedName = metadata.getQualifiedName();
					if (null == metadata.getId()) {
						metadata.setId(new ObjectId());
					}
					((DAGDataServiceImpl) dagDataService).setMetaDataMap(metadata);
					((List<MetadataInstancesDto>) insertMetadata).add(metadata);
					TapTable tapTable = ((DAGDataServiceImpl) dagDataService).getTapTable(qualifiedName);
					if (tapTableMap.containsKey(tableName)) {
						tapTableMap.put(tableName, tapTable);
					} else {
						tapTableMap.putNew(tableName, tapTable, qualifiedName);
					}
				} else {
					throw new RuntimeException("Cannot found metadata from insert metadata list, table name: " + tableName);
				}
			}
		} else if (tapEvent instanceof TapDropTableEvent) {
			// do nothing
		} else {
			TapTable tapTable = ((DAGDataServiceImpl) dagDataService).getTapTable(qualifiedName);
			tapTableMap.put(tableName, tapTable);
			Object updateMetadataObj = tapEvent.getInfo(UPDATE_METADATA_INFO_KEY);
			if (updateMetadataObj instanceof Map) {
				MetadataInstancesDto metadata = ((DAGDataServiceImpl) dagDataService).getMetadata(qualifiedName);
				if (null == metadata.getId()) {
					Object qualifiedNameIdMap = tapEvent.getInfo(QUALIFIED_NAME_ID_MAP_INFO_KEY);
					if (qualifiedNameIdMap instanceof Map) {
						Object id = ((Map<?, ?>) qualifiedNameIdMap).get(qualifiedName);
						if (id instanceof String && StringUtils.isNotBlank((String) id)) {
							metadata.setId(new ObjectId((String) id));
						}
					}
					if (null == metadata.getId()) {
						throw new RuntimeException("Transform result metadata is invalid, id is null");
					}
				}
				((Map<String, MetadataInstancesDto>) updateMetadataObj).put(metadata.getId().toHexString(), metadata);
			}
		}
	}

	protected void updateNodeConfig() {
	}

	protected String getTgtTableNameFromTapEvent(TapEvent tapEvent) {
		return TapEventUtil.getTableId(tapEvent);
	}

	public static class TapValueTransform {
		private Map<String, TapValue<?, ?>> before;
		private Map<String, TapValue<?, ?>> after;

		private TapValueTransform() {
		}

		public static TapValueTransform create() {
			return new TapValueTransform();
		}

		public TapValueTransform before(Map<String, TapValue<?, ?>> before) {
			this.before = before;
			return this;
		}

		public TapValueTransform after(Map<String, TapValue<?, ?>> after) {
			this.after = after;
			return this;
		}

		public Map<String, TapValue<?, ?>> getBefore() {
			return before;
		}

		public Map<String, TapValue<?, ?>> getAfter() {
			return after;
		}
	}

	@Nullable
	protected JobStatus getJetJobStatus() {
		if (null == jetJobStatusMonitor) {
			return null;
		}
		return jetJobStatusMonitor.get();
	}

	protected boolean isJetJobRunning() {
		JobStatus jetJobStatus = getJetJobStatus();
		return null == jetJobStatus || jetJobStatus.equals(JobStatus.RUNNING);
	}
}

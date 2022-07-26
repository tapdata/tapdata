package io.tapdata.flow.engine.V2.node.hazelcast;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.*;
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
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.SettingService;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.entity.OnData;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.flow.engine.V2.common.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.NodeUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.milestone.MilestoneContext;
import io.tapdata.milestone.MilestoneFactory;
import io.tapdata.milestone.MilestoneService;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author jackin
 * @date 2021/12/7 3:25 PM
 **/
public abstract class HazelcastBaseNode extends AbstractProcessor {

	/**
	 * [sub task id]-[node id]
	 */
	private final static String THREAD_NAME_TEMPLATE = "[%s-%s]";
	protected static final String NEW_DAG_INFO_KEY = "NEW_DAG";
	protected static final String DAG_DATA_SERVICE_INFO_KEY = "DAG_DATA_SERVICE";
	protected static final String TRANSFORM_SCHEMA_ERROR_MESSAGE_INFO_KEY = "TRANSFORM_SCHEMA_ERROR_MESSAGE";
	protected static final String UPLOAD_METADATA_INFO_KEY = "UPLOAD_METADATA";
	protected static final String QUALIFIED_NAME_ID_MAP_INFO_KEY = "QUALIFIED_NAME_ID_MAP";
	private static final String TAG = HazelcastBaseNode.class.getSimpleName();

	//  protected BaseMetrics taskNodeMetrics;
//  protected ScheduledExecutorService metricsThreadPool;
//  protected ScheduledFuture<?> metricsThreadPoolFuture;
	protected ClientMongoOperator clientMongoOperator;
	protected Context jetContext;
	protected SettingService settingService;

	protected Map<String, String> tags;
	protected SampleCollector sampleCollector;
	protected SampleCollector statisticCollector;
	protected MilestoneService milestoneService;
	protected Throwable error;
	protected String errorMessage;
	protected ProcessorBaseContext processorBaseContext;
	protected String threadName;

	public AtomicBoolean running = new AtomicBoolean(false);
	protected TapCodecsFilterManager codecsFilterManager;

	/**
	 * Whether to process data from multiple tables
	 */
	protected final boolean multipleTables;

	public HazelcastBaseNode(ProcessorBaseContext processorBaseContext) {
		this.processorBaseContext = processorBaseContext;

		if (null != processorBaseContext.getConfigurationCenter()) {
			this.clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);

			this.settingService = new SettingService(clientMongoOperator);
		}
		if (null != processorBaseContext.getNode() && null == processorBaseContext.getNode().getGraph()) {
			Dag dag = new Dag(processorBaseContext.getEdges(), processorBaseContext.getNodes());
			DAG _DAG = DAG.build(dag);
			_DAG.setTaskId(processorBaseContext.getSubTaskDto().getParentId());
			processorBaseContext.getSubTaskDto().setDag(_DAG);
		}

		threadName = String.format(THREAD_NAME_TEMPLATE, processorBaseContext.getSubTaskDto().getId().toHexString(), processorBaseContext.getNode() != null ? processorBaseContext.getNode().getName() : null);

		//如果为迁移任务、且源节点为数据库类型
		this.multipleTables = CollectionUtils.isNotEmpty(processorBaseContext.getSubTaskDto().getDag().getSourceNode());
	}
	public <T extends DataFunctionAspect<T>> AspectInterceptResult executeDataFuncAspect(Class<T> aspectClass, Callable<T> aspectCallable, Consumer<T> consumer) {
		return AspectUtils.executeDataFuncAspect(aspectClass, aspectCallable, consumer);
	}
	public <T extends Aspect> AspectInterceptResult executeAspect(Class<T> aspectClass, Callable<T> aspectCallable) {
		return AspectUtils.executeAspect(aspectClass, aspectCallable);
	}

	public <T extends Aspect> AspectInterceptResult executeAspect(T aspect) {
		return AspectUtils.executeAspect(aspect);
	}

	protected void doInit(@NotNull Processor.Context context) throws Exception {
	};
	@Override
	public final void init(@NotNull Processor.Context context) throws Exception {
		this.jetContext = context;
		super.init(context);
		Log4jUtil.setThreadContext(processorBaseContext.getSubTaskDto());
		running.compareAndSet(false, true);
		TapCodecsRegistry tapCodecsRegistry = TapCodecsRegistry.create();
		tapCodecsRegistry.registerFromTapValue(TapDateTimeValue.class, tapValue -> tapValue.getValue().toInstant());
		codecsFilterManager = TapCodecsFilterManager.create(tapCodecsRegistry);
		initSampleCollector();
//		CollectorFactory.getInstance().recordCurrentValueByTag(tags);

		doInit(context);
		if(processorBaseContext instanceof DataProcessorContext) {
			AspectUtils.executeAspect(DataNodeInitAspect.class, () -> new DataNodeInitAspect().dataProcessorContext((DataProcessorContext) processorBaseContext));
		} else {
			AspectUtils.executeAspect(ProcessorNodeInitAspect.class, () -> new ProcessorNodeInitAspect().processorBaseContext(processorBaseContext));
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

	protected void transformFromTapValue(TapdataEvent tapdataEvent, Map<String, TapField> sourceNameFieldMap) {
		if (null == tapdataEvent.getTapEvent()) return;
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		if (MapUtils.isNotEmpty(before)) {
			if (null == sourceNameFieldMap) codecsFilterManager.transformFromTapValueMap(before);
			else codecsFilterManager.transformFromTapValueMap(before, sourceNameFieldMap);
		}
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (MapUtils.isNotEmpty(after)) {
			if (null == sourceNameFieldMap) codecsFilterManager.transformFromTapValueMap(after);
			else codecsFilterManager.transformFromTapValueMap(after, sourceNameFieldMap);
		}
	}

	protected void transformToTapValue(TapdataEvent tapdataEvent, TapTableMap<String, TapTable> tapTableMap, String tableName) {
		if (!(tapdataEvent.getTapEvent() instanceof TapRecordEvent)) return;
		if (null == tapTableMap)
			throw new IllegalArgumentException("Transform to TapValue failed, tapTableMap is empty, table name: " + tableName);
		TapTable tapTable = tapTableMap.get(tableName);
		if (null == tapTable)
			throw new IllegalArgumentException("Transform to TapValue failed, table schema is empty, table name: " + tableName);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (MapUtils.isEmpty(nameFieldMap))
			throw new IllegalArgumentException("Transform to TapValue failed, field map is empty, table name: " + tableName);
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		if (MapUtils.isNotEmpty(before)) codecsFilterManager.transformToTapValueMap(before, nameFieldMap);
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (MapUtils.isNotEmpty(after)) codecsFilterManager.transformToTapValueMap(after, nameFieldMap);
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

	protected boolean offer(TapdataEvent dataEvent) {

		if (dataEvent != null) {
			if (processorBaseContext.getNode() != null) {
				dataEvent.addNodeId(processorBaseContext.getNode().getId());
			}
			Outbox outbox = getOutbox();
			if (null == outbox) {
				return true;
			}
			final int bucketCount = outbox.bucketCount();
			if (bucketCount > 1) {
				for (int ordinal = 0; ordinal < bucketCount; ordinal++) {
					final TapdataEvent cloneEvent = (TapdataEvent) dataEvent.clone();
					if (!tryEmit(ordinal, cloneEvent)) {
						return false;
					}
				}
			} else {
				return tryEmit(dataEvent);
			}
		}
		return true;
	}

	protected DataFlow convertTask2DataFlow(ProcessorBaseContext processorBaseContext) {
		SubTaskDto subTaskDto = processorBaseContext.getSubTaskDto();
		Node<?> node = processorBaseContext.getNode();
		List<Node> nodes = processorBaseContext.getNodes();
		List<Edge> edges = processorBaseContext.getEdges();
		DataFlow dataFlow = new DataFlow();
		dataFlow.setId(subTaskDto.getId().toHexString());
		dataFlow.setName(subTaskDto.getName());
		dataFlow.setStatus(subTaskDto.getStatus());
		dataFlow.setTaskId(subTaskDto.getParentId().toHexString());
		dataFlow.setSubTaskId(subTaskDto.getId().toHexString());
		dataFlow.setUser_id(subTaskDto.getUserId());
		if (node instanceof DatabaseNode) {
			dataFlow.setMappingTemplate(ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE);
		} else {
			dataFlow.setMappingTemplate(ConnectorConstant.MAPPING_TEMPLATE_CUSTOM);
		}

		DataFlowSetting setting = new DataFlowSetting();
		setting.setCdcConcurrency(subTaskDto.getParentTask().getIncreSyncConcurrency());
		setting.setIsOpenAutoDDL(subTaskDto.getParentTask().getIsOpenAutoDDL());
		setting.setIsSchedule(subTaskDto.getParentTask().getIsSchedule());
		setting.setStopOnError(subTaskDto.getParentTask().getIsStopOnError());
		setting.setTransformerConcurrency(8);
		setting.setReadCdcInterval(500);

		setting.setCdcFetchSize(subTaskDto.getParentTask().getIncreaseReadSize());
		setting.setCdcShareFilterOnServer(!subTaskDto.getParentTask().getIsFilter());
		setting.setCronExpression(subTaskDto.getParentTask().getCrontabExpression());
		setting.setDistinctWriteType("force".equals(subTaskDto.getParentTask().getDeduplicWriteMode()) ? ConnectorConstant.DISTINCT_WRITE_TYPE_COMPEL : ConnectorConstant.DISTINCT_WRITE_TYPE_INTELLECT);

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
		setting.setNeedToCreateIndex(subTaskDto.getParentTask().getIsAutoCreateIndex());
		//todo
		setting.setProcessorConcurrency(1);
		setting.setReadBatchSize(100);
		setting.setSync_type(subTaskDto.getParentTask().getType());
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
	};
	@Override
	public final void close() throws Exception {
		try {
			doClose();
		} finally {
			running.set(false);
			if(processorBaseContext instanceof DataProcessorContext) {
				AspectUtils.executeAspect(DataNodeCloseAspect.class, () -> new DataNodeCloseAspect().dataProcessorContext((DataProcessorContext) processorBaseContext));
			} else {
				AspectUtils.executeAspect(ProcessorNodeCloseAspect.class, () -> new ProcessorNodeCloseAspect().processorBaseContext(processorBaseContext));
			}
//		InstanceFactory.instance(AspectManager.class).executeAspect(DataNodeCloseAspect.class, () -> new DataNodeCloseAspect().node(HazelcastBaseNode.this));
			if (processorBaseContext.getSubTaskDto() != null) {
				if (sampleCollector != null) {
					CollectorFactory.getInstance().unregisterSampleCollectorFromGroup(processorBaseContext.getSubTaskDto().getId().toString(), sampleCollector);
				}
				if (statisticCollector != null) {
					CollectorFactory.getInstance().unregisterStatisticCollectorFromGroup(processorBaseContext.getSubTaskDto().getId().toString(), statisticCollector);
				}
			} else {
				if (sampleCollector != null) {
					sampleCollector.stop();
					CollectorFactory.getInstance().removeSampleCollectorByTags(sampleCollector.tags());
				}
				if (statisticCollector != null) {
					statisticCollector.stop();
					CollectorFactory.getInstance().removeStatisticCollectorByTags(statisticCollector.tags());
				}
			}
			ThreadContext.clearAll();
			super.close();
			if (error != null) {
				throw new RuntimeException(errorMessage, error);
			}
		}
	}

	public void setMilestoneService(MilestoneService milestoneService) {
		this.milestoneService = milestoneService;
	}

	protected void initSampleCollector() {
		// new version of metrics and statistics collector
		tags = new HashMap<>();
		if (processorBaseContext.getNode() != null) {
			tags.put("nodeId", processorBaseContext.getNode().getId());
			tags.put("type", "node");
		}
		if (processorBaseContext.getSubTaskDto() != null) {
			tags.put("subTaskId", processorBaseContext.getSubTaskDto().getId().toString());
			tags.put("taskId", processorBaseContext.getSubTaskDto().getParentId().toString());
		}
		sampleCollector = CollectorFactory.getInstance().getSampleCollectorByTags("nodeSamples", tags);
		statisticCollector = CollectorFactory.getInstance().getStatisticCollectorByTags("nodeStatistics", tags);
		if (processorBaseContext.getSubTaskDto() != null) {
			CollectorFactory.getInstance().registerSampleCollectorToGroup(processorBaseContext.getSubTaskDto().getId().toString(), sampleCollector);
			CollectorFactory.getInstance().registerStatisticCollectorToGroup(processorBaseContext.getSubTaskDto().getId().toString(), statisticCollector);
		}
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
		this.milestoneService = MilestoneFactory.getJetEdgeMilestoneService(processorBaseContext.getSubTaskDto(), httpClientMongoOperator.getRestTemplateOperator().getBaseURLs(), httpClientMongoOperator.getRestTemplateOperator().getRetryTime(), httpClientMongoOperator.getConfigCenter(), node, vertexName, vertexNames, null, vertexType);
	}

	protected void errorHandle(Throwable throwable, String errorMessage) {
		this.error = throwable;
		this.errorMessage = errorMessage;
		SubTaskDto subTaskDto = processorBaseContext.getSubTaskDto();
		com.hazelcast.jet.Job hazelcastJob = jetContext.hazelcastInstance().getJet().getJob(subTaskDto.getName() + "-" + subTaskDto.getId().toHexString());
		if (hazelcastJob != null) {
			AspectUtils.executeAspect(new TaskStopAspect().task(subTaskDto).error(throwable));
			hazelcastJob.cancel();
		}
	}

	protected boolean taskHasBeenRun() {
		final SubTaskDto subTaskDto = processorBaseContext.getSubTaskDto();
		if (subTaskDto != null && MapUtils.isNotEmpty(subTaskDto.getAttrs())) {
			return subTaskDto.getAttrs().containsKey("syncProgress");
		}

		return false;
	}

	@Override
	public boolean isCooperative() {
		return false;
	}

	protected boolean isRunning() {
		return running.get() && !Thread.currentThread().isInterrupted();
	}

	protected void updateMemoryFromDDLInfoMap(TapdataEvent tapdataEvent) {
		updateMemoryFromDDLInfoMap(tapdataEvent, null);
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
		processorBaseContext.getSubTaskDto().setDag((DAG) newDAG);
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
		TapTable tapTable = ((DAGDataServiceImpl) dagDataService).getTapTable(qualifiedName);
		tapTableMap.put(tableName, tapTable);
		Object uploadMetadataObj = tapEvent.getInfo(UPLOAD_METADATA_INFO_KEY);
		if (uploadMetadataObj instanceof Map) {
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
			((Map<String, MetadataInstancesDto>) uploadMetadataObj).put(metadata.getId().toHexString(), metadata);
		}
	}

	protected void updateNodeConfig() {
	}
}

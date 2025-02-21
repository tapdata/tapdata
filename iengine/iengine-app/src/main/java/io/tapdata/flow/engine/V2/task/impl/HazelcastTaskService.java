package io.tapdata.flow.engine.V2.task.impl;

import cn.hutool.core.collection.CollUtil;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.tapdata.cache.ICacheService;
import com.tapdata.cache.external.ExternalStorageCacheService;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.JetDag;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.entity.task.error.TaskServiceExCode_23;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.exception.AutoInspectException;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.*;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.taskmilestones.EngineDeductionAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.autoinspect.utils.AutoInspectNodeUtil;
import io.tapdata.common.DAGDataEngineServiceImpl;
import io.tapdata.common.SettingService;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.dao.MessageDao;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.cleaner.impl.MergeNodeCleaner;
import io.tapdata.flow.engine.V2.entity.GlobalConstant;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.entity.TaskEnvMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderController;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderControllerExCode_21;
import io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderService;
import io.tapdata.flow.engine.V2.node.hazelcast.data.*;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewInstance;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewService;
import io.tapdata.flow.engine.V2.task.preview.entity.PreviewConnectionInfo;
import io.tapdata.flow.engine.V2.task.preview.node.HazelcastPreviewMergeNode;
import io.tapdata.flow.engine.V2.task.preview.node.HazelcastPreviewSourcePdkDataNode;
import io.tapdata.flow.engine.V2.task.preview.node.HazelcastPreviewTargetNode;
import io.tapdata.flow.engine.V2.util.*;
import io.tapdata.flow.engine.util.TaskDtoUtil;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author jackin
 * @date 2021/12/1 8:56 PM
 **/
@Service
@DependsOn("tapdataTaskScheduler")
public class HazelcastTaskService implements TaskService<TaskDto> {

	private static final Logger logger = LogManager.getLogger(HazelcastTaskService.class);
	private static final String TAG = HazelcastTaskService.class.getSimpleName();
	public static final int DEFAULT_JET_EDGE_QUEUE_SIZE = 128;
	public static final String JET_EDGE_QUEUE_SIZE_PROP_KEY = "JET_EDGE_QUEUE_SIZE";

	private static HazelcastInstance hazelcastInstance;
	private static HazelcastTaskService taskService;

	@Autowired
	private ConfigurationCenter configurationCenter;

	private static ClientMongoOperator clientMongoOperator;

	@Autowired
	private SettingService settingService;

	@Autowired
	private MessageDao messageDao;

	private static ICacheService cacheService;

	public HazelcastTaskService(ClientMongoOperator clientMongoOperator) {
		if (HazelcastTaskService.clientMongoOperator == null) {
			HazelcastTaskService.clientMongoOperator = clientMongoOperator;
		}
		taskService = this;
	}

	public static HazelcastTaskService taskService() {
		return taskService;
	}

	@PostConstruct
	public void init() {
		String agentId = (String) configurationCenter.getConfig(ConfigurationCenter.AGENT_ID);
		Config config = HazelcastUtil.getConfig(agentId);
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		cacheService = new ExternalStorageCacheService(hazelcastInstance, clientMongoOperator);
		messageDao.setCacheService(cacheService);
		GlobalConstant.getInstance().hazelcastInstance(hazelcastInstance);
	}

	public static HazelcastInstance getHazelcastInstance() {
		return hazelcastInstance;
	}

	@Override
	public TaskClient<TaskDto> startTask(TaskDto taskDto) {
		try {
			taskDto.setDag(taskDto.getDag());
			ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto);

			//Sprint 90- TAP2489: Unchecked errors will be cleared directly upon startup
			cleanAllUnselectedError(taskDto, obsLogger);

			AspectUtils.executeAspect(new TaskStartAspect().task(taskDto).log(InstanceFactory.instance(LogFactory.class).getLog(taskDto)));
			JobConfig jobConfig = new JobConfig();
			jobConfig.setName(taskDto.getName() + "-" + taskDto.getId().toHexString());
			jobConfig.setProcessingGuarantee(ProcessingGuarantee.NONE);
			JetService jet = hazelcastInstance.getJet();
			HazelcastTaskClient hazelcastTaskClient = HazelcastTaskClient.create(taskDto, clientMongoOperator, configurationCenter, hazelcastInstance);
			Job job = startJetJob(taskDto, obsLogger, jet, jobConfig, hazelcastTaskClient);
			hazelcastTaskClient.setJob(job);
			obsLogger.info("Task started");
			return hazelcastTaskClient;
		} catch (Throwable throwable) {
			AspectUtils.executeAspect(new TaskStopAspect().task(taskDto).error(throwable));
			throw throwable;
		}
	}

	private @NotNull Job startJetJob(TaskDto taskDto, ObsLogger obsLogger, JetService jet, JobConfig jobConfig, HazelcastTaskClient hazelcastTaskClient) {
		Job job;
		try {
			final JetDag jetDag = task2HazelcastDAG(taskDto, true);
			obsLogger.trace("The engine receives " + taskDto.getName() + " task data from TM and will continue to run tasks by jet");
			job = jet.newJob(jetDag.getDag(), jobConfig);
		} catch (Exception e) {
			hazelcastTaskClient.close();
			throw e;
		}
		return job;
	}

	protected void cleanAllUnselectedError(TaskDto taskDto, ObsLogger obsLogger) {
		List<ErrorEvent> errorEvents = taskDto.getErrorEvents();
		if (!CollUtil.isEmpty(errorEvents)) {
			List<ErrorEvent> newErrorEvents = errorEvents.stream()
					.filter(Objects::nonNull)
					.filter(ErrorEvent::getSkip)
					.collect(Collectors.toList());
			taskDto.setErrorEvents(newErrorEvents);
			TaskDtoUtil.updateErrorEvent(clientMongoOperator, newErrorEvents, taskDto.getId(), obsLogger, "Task initialization error list error, message: {}");
		}
	}

	@Override
	public TaskClient<TaskDto> startTestTask(TaskDto taskDto) {
		try {
			taskDto.setDag(taskDto.getDag());
			AspectUtils.executeAspect(new TaskStartAspect().task(taskDto).log(new TapLog()));
			long startTs = System.currentTimeMillis();
			final JetDag jetDag = task2HazelcastDAG(taskDto, false);
			JobConfig jobConfig = new JobConfig();
			jobConfig.setProcessingGuarantee(ProcessingGuarantee.NONE);
			logger.info("task2HazelcastDAG cost {}ms", (System.currentTimeMillis() - startTs));
			Job job = hazelcastInstance.getJet().newLightJob(jetDag.getDag(), jobConfig);
			return new HazelcastTaskClient(job, taskDto, clientMongoOperator, configurationCenter, hazelcastInstance);
		} catch (Throwable throwable) {
			ObsLoggerFactory.getInstance().getObsLogger(taskDto).error(throwable);
			AspectUtils.executeAspect(new TaskStopAspect().task(taskDto).error(throwable));
			throw throwable;
		}
	}

	@Override
	public TaskClient<TaskDto> startTestTask(TaskDto taskDto, AtomicReference<Object> result) {
		try {
			taskDto.setDag(taskDto.getDag());
			AspectUtils.executeAspect(new TaskStartAspect().task(taskDto).info("KYE_OF_SCRIPT_RUN_RESULT", result).log(new TapLog()));
			long startTs = System.currentTimeMillis();
			final JetDag jetDag = task2HazelcastDAG(taskDto, false);
			JobConfig jobConfig = new JobConfig();
			jobConfig.setProcessingGuarantee(ProcessingGuarantee.NONE);
			logger.info("task2HazelcastDAG cost {}ms", (System.currentTimeMillis() - startTs));
			Job job = hazelcastInstance.getJet().newLightJob(jetDag.getDag(), jobConfig);
			return new HazelcastTaskClient(job, taskDto, clientMongoOperator, configurationCenter, hazelcastInstance);
		} catch (Throwable throwable) {
			ObsLoggerFactory.getInstance().getObsLogger(taskDto).error(throwable);
			AspectUtils.executeAspect(new TaskStopAspect().task(taskDto).error(throwable));
			throw throwable;
		}

	}

	@Override
	public TaskClient<TaskDto> startPreviewTask(TaskDto taskDto) {
		final JetDag jetDag = task2HazelcastDAG(taskDto, true);
		JobConfig jobConfig = new JobConfig();
		jobConfig.setProcessingGuarantee(ProcessingGuarantee.NONE);
		Job job = hazelcastInstance.getJet().newLightJob(jetDag.getDag(), jobConfig);
		return new HazelcastTaskClient(job, taskDto, clientMongoOperator, configurationCenter, hazelcastInstance);
	}

	@SneakyThrows
	protected JetDag task2HazelcastDAG(TaskDto taskDto, Boolean deduce) {
		Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap;
		if (deduce) {
			if (taskDto.isPreviewTask()) {
				tapTableMapHashMap = transformSchemaWhenPreview(taskDto);
			} else {
				tapTableMapHashMap = engineTransformSchema(taskDto);
			}
		} else {
			tapTableMapHashMap = new HashMap<>();
		}
		initEnv(taskDto, hazelcastInstance);
		DAG dag = new DAG();
		AtomicReference<TaskDto> taskDtoAtomicReference = new AtomicReference<>(taskDto);
		Long tmCurrentTime = taskDtoAtomicReference.get().getTmCurrentTime();
		if (null != tmCurrentTime && tmCurrentTime.compareTo(0L) > 0 && taskDto.isNormalTask()) {
			Map<String, Object> params = new HashMap<>();
			params.put("id", taskDto.getId().toHexString());
			params.put("time", tmCurrentTime);
			clientMongoOperator.deleteByMap(params, ConnectorConstant.TASK_COLLECTION + "/history");
			TaskDto taskDtoByMongoFind = clientMongoOperator.findOne(params, ConnectorConstant.TASK_COLLECTION + "/history", TaskDto.class);
			taskDtoByMongoFind.setDag(taskDtoByMongoFind.getDag());
			taskDtoAtomicReference.set(taskDtoByMongoFind);
			if (null == taskDtoAtomicReference.get()) {
				throw new RuntimeException("Get task history failed, param: " + params + ", result is null");
			}
		}

		TaskConfig taskConfig = getTaskConfig(taskDtoAtomicReference.get());
		if (taskDto.isNormalTask()) {
			initSourceInitialCounter(taskDtoAtomicReference.get());
			// init snapshot order (only for normal task)
			initSnapshotOrder(taskDtoAtomicReference);
		}

		final List<Node> nodes = taskDtoAtomicReference.get().getDag().getNodes();
		final List<Edge> edges = taskDtoAtomicReference.get().getDag().getEdges();
		Map<String, Vertex> vertexMap = new HashMap<>();
		Map<String, AbstractProcessor> hazelcastBaseNodeMap = new HashMap<>();
		Map<String, AbstractProcessor> typeConvertMap = new HashMap<>();
		Map<String, Node<?>> nodeMap = nodes.stream().collect(Collectors.toMap(Element::getId, n -> n));

		final ConfigurationCenter config = (ConfigurationCenter) configurationCenter.clone();
		if (CollectionUtils.isNotEmpty(nodes)) {

			// Get merge table map
			Map<String, MergeTableNode> mergeTableMap = MergeTableUtil.getMergeTableMap(nodes, edges);

			// Generate AutoInspectNode
			try {
				AutoInspectNode inspectNode = AutoInspectNodeUtil.firstAutoInspectNode(taskDtoAtomicReference.get());
				if (null != inspectNode) {
					ObsLoggerFactory.getInstance().getObsLogger(taskDto).info("Enable automatic data verification");
					nodes.add(inspectNode);
					nodeMap.put(inspectNode.getId(), inspectNode);
					edges.add(new Edge(inspectNode.getFromNode().getId(), inspectNode.getId()));
				}
			} catch (AutoInspectException e) {
				ObsLoggerFactory.getInstance().getObsLogger(taskDtoAtomicReference.get()).warn(e.getMessage());
			}

			AtomicBoolean needFilterEvent = new AtomicBoolean(true);
			for (Node node : nodes) {
				Connections connection = null;
				TableNode tableNode = null;
				DatabaseTypeEnum.DatabaseType databaseType = null;
				TapTableMap<String, TapTable> tapTableMap = getTapTableMap(taskDto, tmCurrentTime, node, tapTableMapHashMap);
				if (CollectionUtils.isEmpty(tapTableMap.keySet())
						&& !(node instanceof CacheNode)
						&& !(node instanceof HazelCastImdgNode)
						&& !(node instanceof TableRenameProcessNode)
						&& !(node instanceof MigrateFieldRenameProcessorNode)
						&& !(node instanceof MigrateDateProcessorNode)
						&& !(node instanceof VirtualTargetNode)
						&& !(node instanceof PreviewTargetNode)
						&& taskDto.isNormalTask()
				) {
					throw new NodeException(String.format("Node [id %s, name %s] schema cannot be empty",
							node.getId(), node.getName()));
				}

				if (node instanceof DataParentNode) {
					if (taskDto.isPreviewTask()) {
						TaskPreviewInstance taskPreviewInstance = TaskPreviewService.taskPreviewInstance(taskDto);
						Map<String, PreviewConnectionInfo> nodeConnectionInfoMap = taskPreviewInstance.getNodeConnectionInfoMap();
						PreviewConnectionInfo previewConnectionInfo = nodeConnectionInfoMap.get(((DataParentNode<?>) node).getConnectionId());
						connection = previewConnectionInfo.getConnections();
						databaseType = previewConnectionInfo.getDatabaseType();
					} else {
						connection = getConnection(((DataParentNode<?>) node).getConnectionId());
						databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());
					}
					tableNode = node instanceof TableNode ? (TableNode) node : null;
				} else if (node.isLogCollectorNode()) {
					LogCollectorNode logCollectorNode = (LogCollectorNode) node;
					String connectionId = logCollectorNode.getConnectionIds().get(0);
					connection = getConnection(connectionId);
					databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());

					ShareCdcUtil.fillConfigNamespace(logCollectorNode, this::getConnection);
				} else if (node instanceof CacheNode) {
					Optional<Edge> edge = edges.stream().filter(e -> e.getTarget().equals(node.getId())).findFirst();
					Node<?> sourceNode = null;
					if (edge.isPresent()) {
						sourceNode = nodeMap.get(edge.get().getSource());
						if (sourceNode instanceof TableNode) {
							connection = getConnection(((TableNode) sourceNode).getConnectionId());
						}
					}
					messageDao.registerCache((CacheNode) node, (TableNode) sourceNode, connection, taskDtoAtomicReference.get(), clientMongoOperator);
				} else if (node instanceof MergeTableNode){
					cleanMergeNode(taskDto,node.getId());
				}
				List<Node> predecessors = node.predecessors();
				List<Node> successors = node.successors();
				Connections finalConnection = connection;
				DatabaseTypeEnum.DatabaseType finalDatabaseType = databaseType;
				TapTableMap<String, TapTable> finalTapTableMap = tapTableMap;
				Vertex vertex = new Vertex(NodeUtil.getVertexName(node), () -> {
					try {
						TaskDto dto = taskDtoAtomicReference.get();
						dto.setNeedFilterEventData(needFilterEvent.get());
						HazelcastBaseNode hazelcastBaseNode = createNode(
								dto,
								nodes,
								edges,
								node,
								predecessors,
								successors,
								config,
								finalConnection,
								finalDatabaseType,
								mergeTableMap,
								finalTapTableMap,
								taskConfig
						);
						return hazelcastBaseNode;
					} catch (Exception e) {
						throw new TapCodeException(TaskProcessorExCode_11.CREATE_PROCESSOR_FAILED,
								String.format("Failed to create processor based on node information, node: %s[%s], error msg: %s", node.getName(), node.getId(), e.getMessage()), e);
					}
				});
				vertexMap.put(node.getId(), vertex);

				vertex.localParallelism(1);
				dag.vertex(vertex);
				this.singleTaskFilterEventDataIfNeed(connection, needFilterEvent, tableNode);
			}
			handleEdge(dag, edges, nodeMap, vertexMap);
		}

		return new JetDag(dag, hazelcastBaseNodeMap, typeConvertMap);
	}

	protected Map<String, TapTableMap<String, TapTable>> transformSchemaWhenPreview(TaskDto taskDto) {
		Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap = null;
		TaskPreviewInstance taskPreviewInstance = TaskPreviewService.taskPreviewInstance(taskDto);
		if (null != taskPreviewInstance) {
			tapTableMapHashMap = taskPreviewInstance.getTapTableMapHashMap();
		}
		if (MapUtils.isEmpty(tapTableMapHashMap)) {
			tapTableMapHashMap = engineTransformSchema(taskDto);
		}
		return tapTableMapHashMap;
	}

	protected void singleTaskFilterEventDataIfNeed(Connections conn, AtomicBoolean needFilterEvent, TableNode tableNode) {
		if (null == conn || null == needFilterEvent) return;
		List<String> tags = conn.getDefinitionTags();
		if (Boolean.TRUE.equals(needFilterEvent.get())) {
			boolean isCustomCommand = null != tableNode && tableNode.isEnableCustomCommand();
			needFilterEvent.set(null == tags || (!tags.contains("schema-free") && !isCustomCommand));
		}
	}

	protected static void initSnapshotOrder(AtomicReference<TaskDto> taskDtoAtomicReference) {
		try {
			SnapshotOrderService snapshotOrderService = SnapshotOrderService.getInstance();
			SnapshotOrderController snapshotOrderController = snapshotOrderService.addController(taskDtoAtomicReference.get());
			snapshotOrderController.flush();
			ObsLoggerFactory.getInstance().getObsLogger(taskDtoAtomicReference.get()).trace(snapshotOrderService.getController(taskDtoAtomicReference.get().getId().toHexString()).toString());
		} catch (Exception e) {
			throw new TapCodeException(SnapshotOrderControllerExCode_21.UNKNOWN_ERROR, e);
		}
	}

	protected static TapTableMap<String, TapTable> getTapTableMap(TaskDto taskDto, Long tmCurrentTime, Node node, Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap) {
		TapTableMap<String, TapTable> tapTableMap;
		if (node instanceof AutoInspectNode) {
			tapTableMap = TapTableUtil.getTapTableMapByNodeId(AutoInspectConstants.MODULE_NAME, ((AutoInspectNode) node).getTargetNodeId(), System.currentTimeMillis());
		} else if (StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(),
//						TaskDto.SYNC_TYPE_TEST_RUN,
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
			tapTableMap = TapTableUtil.getTapTableMap(node, tmCurrentTime);
		} else if (node instanceof VirtualTargetNode) {
			tapTableMap = TapTableMap.create(node.getId());
		} else if (StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(), TaskDto.SYNC_TYPE_TEST_RUN)) {
			tapTableMap = TapTableUtil.getTapTableMapByNodeId(node.getId(), tmCurrentTime);
		} else {
			if (tapTableMapHashMap.containsKey(node.getId())) {
				tapTableMap = tapTableMapHashMap.get(node.getId());
			} else {
				tapTableMap = TapTableMap.create(node.getId());
			}

		}
		return tapTableMap;
	}

	public static HazelcastBaseNode createNode(
			TaskDto taskDto,
			List<Node> nodes,
			List<Edge> edges,
			Node node,
			List<Node> predecessors,
			List<Node> successors,
			ConfigurationCenter config,
			Connections connection,
			DatabaseTypeEnum.DatabaseType databaseType,
			Map<String, MergeTableNode> mergeTableMap,
			TapTableMap<String, TapTable> tapTableMap,
			TaskConfig taskConfig
	) throws Exception {
		List<RelateDataBaseTable> nodeSchemas = new ArrayList<>();
		if (!StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(), TaskDto.SYNC_TYPE_TEST_RUN, TaskDto.SYNC_TYPE_DEDUCE_SCHEMA) &&
				(node instanceof ProcessorNode || node instanceof MigrateProcessorNode) && node.disabledNode()) {
			HazelcastBlank newNode = new HazelcastBlank(
					DataProcessorContext.newBuilder()
							.withTaskDto(taskDto)
							.withNode(node)
							.withNodeSchemas(nodeSchemas)
							.withTapTableMap(tapTableMap)
							.withTaskConfig(taskConfig)
							.build()
			);
			MergeTableUtil.setMergeTableIntoHZTarget(mergeTableMap, newNode);
			return newNode;
		}
		HazelcastBaseNode hazelcastNode;
		final String type = node.getType();
		final NodeTypeEnum nodeTypeEnum = NodeTypeEnum.get(type);
		boolean previewTask = taskDto.isPreviewTask();
		switch (nodeTypeEnum) {
			case DATABASE:
			case TABLE:
				if (CollectionUtils.isNotEmpty(predecessors) && CollectionUtils.isNotEmpty(successors)) {
					if ("pdk".equals(connection.getPdkType())) {
						hazelcastNode = new HazelcastPdkSourceAndTargetTableNode(
								DataProcessorContext.newBuilder()
										.withTaskDto(taskDto)
										.withNode(node)
										.withNodes(nodes)
										.withEdges(edges)
										.withConfigurationCenter(config)
										.withConnections(connection)
										.withConnectionConfig(connection.getConfig())
										.withDatabaseType(databaseType)
										.withTapTableMap(tapTableMap)
										.withTaskConfig(taskConfig)
										.build()
						);
					} else {
						hazelcastNode = new HazelcastTaskSourceAndTarget(
								DataProcessorContext.newBuilder()
										.withTaskDto(taskDto)
										.withNode(node)
										.withNodes(nodes)
										.withEdges(edges)
										.withConfigurationCenter(config)
										.withConnections(connection)
										.withCacheService(cacheService)
										.build());
					}
				} else if (CollectionUtils.isNotEmpty(successors)) {
					if ("pdk".equals(connection.getPdkType())) {
						DataProcessorContext processorContext = DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withSourceConn(connection)
								.withConnections(connection)
								.withConnectionConfig(connection.getConfig())
								.withConnections(connection)
								.withDatabaseType(databaseType)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build();
						if (StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(),
								TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
							hazelcastNode = new HazelcastSampleSourcePdkDataNode(processorContext);
						} else {
							ReadPartitionOptions readPartitionOptions = null;
							if (node instanceof DataParentNode) {
								readPartitionOptions = ((DataParentNode<?>) node).getReadPartitionOptions();
							}

							if (readPartitionOptions != null && readPartitionOptions.isEnable() && readPartitionOptions.getSplitType() != ReadPartitionOptions.SPLIT_TYPE_NONE && !Objects.equals(taskDto.getType(), SyncTypeEnum.CDC.getSyncType())) {
								hazelcastNode = new HazelcastSourcePartitionReadDataNode(processorContext);
							} else if (StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(), TaskDto.SYNC_TYPE_MIGRATE) && node instanceof DatabaseNode && ((DatabaseNode) node).isEnableConcurrentRead()) {
								hazelcastNode = new HazelcastSourceConcurrentReadDataNode(processorContext);
							} else if (previewTask) {
								hazelcastNode = new HazelcastPreviewSourcePdkDataNode(processorContext);
							} else {
								hazelcastNode = new HazelcastSourcePdkDataNode(processorContext);
							}
//							hazelcastNode = new HazelcastSourcePdkDataNode(processorContext);
						}
					} else {
						hazelcastNode = new HazelcastTaskSource(
								DataProcessorContext.newBuilder()
										.withTaskDto(taskDto)
										.withNode(node)
										.withNodes(nodes)
										.withEdges(edges)
										.withConfigurationCenter(config)
										.withSourceConn(connection)
										.build());
					}
				} else {
					if ("pdk".equals(connection.getPdkType())) {
						DataProcessorContext dataProcessorContext = DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTargetConn(connection)
								.withConnections(connection)
								.withConnectionConfig(connection.getConfig())
								.withDatabaseType(databaseType)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build();
						hazelcastNode = new HazelcastTargetPdkDataNode(dataProcessorContext);
					} else {
						hazelcastNode = new HazelcastTaskTarget(
								DataProcessorContext.newBuilder()
										.withTaskDto(taskDto)
										.withNode(node)
										.withNodes(nodes)
										.withEdges(edges)
										.withConfigurationCenter(config)
										.withNodeSchemas(nodeSchemas)
										.withTargetConn(connection)
										.withCacheService(cacheService)
										.build()
						);
					}
				}
				break;
			case CACHE:
				if ("pdk".equals(connection.getPdkType())) {
					hazelcastNode = new HazelcastTargetPdkCacheNode(
							DataProcessorContext.newBuilder()
									.withTaskDto(taskDto)
									.withNode(node)
									.withNodes(nodes)
									.withEdges(edges)
									.withConfigurationCenter(config)
									.withConnectionConfig(connection.getConfig())
									.withDatabaseType(databaseType)
									.withTapTableMap(tapTableMap)
									.withCacheService(cacheService)
									.withTaskConfig(taskConfig)
									.build()
					);
				} else {
					hazelcastNode = new HazelcastCacheTarget(
							DataProcessorContext.newBuilder()
									.withTaskDto(taskDto)
									.withNode(node)
									.withNodes(nodes)
									.withEdges(edges)
									.withConfigurationCenter(config)
									.withTargetConn(connection)
									.withCacheService(cacheService)
									.withTapTableMap(tapTableMap)
									.withTaskConfig(taskConfig)
									.build()
					);
				}
				break;
			case AUTO_INSPECT:
				if ("pdk".equals(connection.getPdkType())) {
					hazelcastNode = new HazelcastTargetPdkAutoInspectNode(
							DataProcessorContext.newBuilder()
									.withTaskDto(taskDto)
									.withNode(node)
									.withNodes(nodes)
									.withEdges(edges)
									.withConfigurationCenter(config)
									.withConnectionConfig(connection.getConfig())
									.withDatabaseType(databaseType)
									.withTapTableMap(tapTableMap)
									.withCacheService(cacheService)
									.withTaskConfig(taskConfig)
									.build()
					);
				} else {
					throw new RuntimeException("un support AutoInspect node " + connection.getPdkType());
				}
				break;
			case VIRTUAL_TARGET:
				DataProcessorContext processorContext = DataProcessorContext.newBuilder()
						.withTaskDto(taskDto)
						.withNode(node)
						.withNodes(nodes)
						.withEdges(edges)
						.withConfigurationCenter(config)
						.withTargetConn(connection)
						.withCacheService(cacheService)
						.withTapTableMap(tapTableMap)
						.withTaskConfig(taskConfig)
						.build();
				if (TaskDto.SYNC_TYPE_TEST_RUN.equals(taskDto.getSyncType())) {
					hazelcastNode = new HazelcastVirtualTargetNode(processorContext);
				} else {
					hazelcastNode = new HazelcastSchemaTargetNode(processorContext);
				}

				break;
			case JOIN:
				hazelcastNode = new HazelcastJoinProcessor(
						ProcessorBaseContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withNodeSchemas(nodeSchemas)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case CACHE_LOOKUP_PROCESSOR:
			case JS_PROCESSOR:
			case STANDARD_JS_PROCESSOR:
			case MIGRATE_JS_PROCESSOR:
			case STANDARD_MIGRATE_JS_PROCESSOR:
				hazelcastNode = new HazelcastJavaScriptProcessorNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withCacheService(cacheService)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case PYTHON_PROCESS:
			case MIGRATE_PYTHON_PROCESS:
				hazelcastNode = new HazelcastPythonProcessNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withCacheService(cacheService)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case UNWIND_PROCESS:
				hazelcastNode = new HazelcastUnwindProcessNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withCacheService(cacheService)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case FIELD_PROCESSOR:
			case ROW_FILTER_PROCESSOR:
			case FIELD_RENAME_PROCESSOR:
			case FIELD_MOD_TYPE_PROCESSOR:
			case FIELD_CALC_PROCESSOR:
			case FIELD_ADD_DEL_PROCESSOR:
				hazelcastNode = new HazelcastProcessorNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withCacheService(cacheService)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case TABLE_RENAME_PROCESSOR:
				hazelcastNode = new HazelcastRenameTableProcessorNode(DataProcessorContext.newBuilder()
						.withTaskDto(taskDto)
						.withNode(node)
						.withNodes(nodes)
						.withEdges(edges)
						.withCacheService(cacheService)
						.withConfigurationCenter(config)
						.withTapTableMap(tapTableMap)
						.withTaskConfig(taskConfig)
						.build());
				break;
			case MIGRATE_FIELD_RENAME_PROCESSOR:
				hazelcastNode = new HazelcastMigrateFieldRenameProcessorNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withCacheService(cacheService)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build());
				break;
			case LOG_COLLECTOR:
				hazelcastNode = new HazelcastSourcePdkShareCDCNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withConnections(connection)
								.withConnectionConfig(connection.getConfig())
								.withDatabaseType(databaseType)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case HAZELCASTIMDG:
				Connections connections = new Connections();
				connections.setDatabase_type(DatabaseTypeEnum.HAZELCAST_IMDG.getType());
				hazelcastNode = new HazelcastTargetPdkShareCDCNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case CUSTOM_PROCESSOR:
				hazelcastNode = new HazelcastCustomProcessor(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case MERGETABLE:
				DataProcessorContext mergeNodeContext = DataProcessorContext.newBuilder()
						.withTaskDto(taskDto)
						.withNode(node)
						.withNodes(nodes)
						.withEdges(edges)
						.withConfigurationCenter(config)
						.withTapTableMap(tapTableMap)
						.withTaskConfig(taskConfig)
						.build();
				if (taskDto.isPreviewTask()) {
					hazelcastNode = new HazelcastPreviewMergeNode(mergeNodeContext);
				} else {
					hazelcastNode = new HazelcastMergeNode(mergeNodeContext);
				}
				break;
			case DATE_PROCESSOR:
			case MIGRATE_DATE_PROCESSOR:
				hazelcastNode = new HazelcastDateProcessorNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case FIELD_MOD_TYPE_FILTER_PROCESSOR:
			case MIGRATE_FIELD_MOD_TYPE_FILTER_PROCESSOR:
				hazelcastNode = new HazelcastTypeFilterProcessorNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case ADD_DATE_FIELD_PROCESS:
			case MIGRATE_ADD_DATE_FIELD_PROCESSOR:
				hazelcastNode = new HazelcastAddDateFieldProcessNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case MIGRATE_UNION_PROCESSOR:
				hazelcastNode = new HazelcastMigrateUnionProcessorNode(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
			case PREVIEW_TARGET:
				DataProcessorContext dataProcessorContext = DataProcessorContext.newBuilder()
						.withTaskDto(taskDto)
						.withNode(node)
						.withNodes(nodes)
						.withEdges(edges)
						.build();
				hazelcastNode = new HazelcastPreviewTargetNode(dataProcessorContext);
				break;
			default:
				hazelcastNode = new HazelcastBlank(
						DataProcessorContext.newBuilder()
								.withTaskDto(taskDto)
								.withNode(node)
								.withTapTableMap(tapTableMap)
								.withTaskConfig(taskConfig)
								.build()
				);
				break;
		}
		MergeTableUtil.setMergeTableIntoHZTarget(mergeTableMap, hazelcastNode);
		return hazelcastNode;
	}

	private void handleEdge(
			DAG dag,
			List<Edge> edges,
			Map<String, Node<?>> nodeMap,
			Map<String, Vertex> vertexMap
	) {
		if (CollectionUtils.isNotEmpty(edges)) {
			for (Edge edge : edges) {
				final String source = edge.getSource();
				final String target = edge.getTarget();
				final Node<?> srcNode = nodeMap.get(source);
				final Node<?> tgtNode = nodeMap.get(target);
				List<com.hazelcast.jet.core.Edge> outboundEdges = dag.getOutboundEdges(NodeUtil.getVertexName(srcNode));
				List<com.hazelcast.jet.core.Edge> inboundEdges = dag.getInboundEdges(NodeUtil.getVertexName(tgtNode));
				int queueSize = CommonUtils.getPropertyInt(JET_EDGE_QUEUE_SIZE_PROP_KEY, DEFAULT_JET_EDGE_QUEUE_SIZE);
				Integer readBatchSize = -1;
				if (srcNode instanceof DataParentNode) {
					readBatchSize = ((DataParentNode<?>) srcNode).getReadBatchSize();
				}
				if (queueSize < readBatchSize) {
					queueSize = readBatchSize;
				}
				EdgeConfig edgeConfig = new EdgeConfig().setQueueSize(queueSize);
				com.hazelcast.jet.core.Edge jetEdge = com.hazelcast.jet.core.Edge
						.from(vertexMap.get(source), outboundEdges.size())
						.to(vertexMap.get(target), inboundEdges.size())
						.setConfig(edgeConfig);
				dag.edge(jetEdge);
			}
		}
	}

	@Override
	public TaskClient getTaskClient(String taskId) {
		return null;
	}

	@Override
	public List<TaskClient<TaskDto>> getTaskClients() {
		return null;
	}

	public Connections getConnection(String connectionId) {
		return ConnectionUtil.getConnection(connectionId, null);
	}

	protected TaskConfig getTaskConfig(TaskDto taskDto) {
		return TaskConfig.create()
				.taskDto(taskDto)
				.taskRetryConfig(getTaskRetryConfig(taskDto))
				.externalStorageDtoMap(ExternalStorageUtil.getExternalStorageMap(taskDto, clientMongoOperator));
	}

	protected TaskRetryConfig getTaskRetryConfig(TaskDto taskDto) {
		long retryIntervalSecond = null == taskDto.getRetryIntervalSecond() ? settingService.getLong("retry_interval_second", 60L) : taskDto.getRetryIntervalSecond();
		long maxRetryTimeMinute = null == taskDto.getMaxRetryTimeMinute() ? settingService.getLong("max_retry_time_minute", 60L) : taskDto.getMaxRetryTimeMinute();
		long maxRetryTimeSecond = maxRetryTimeMinute * 60;
		return TaskRetryConfig.create()
				.retryIntervalSecond(retryIntervalSecond)
				.maxRetryTimeSecond(maxRetryTimeSecond);
	}

	protected void initSourceInitialCounter(TaskDto taskDto) {
		String type = taskDto.getType();
		com.tapdata.tm.commons.dag.DAG dag = taskDto.getDag();
		List<Node> sourceNodes = dag.getSourceNodes();
		Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE.getTaskGlobalVariable(taskDto.getId().toHexString());
		if (TaskDto.TYPE_INITIAL_SYNC.equals(type) || CollectionUtils.isNotEmpty(GraphUtil.findMergeNode(taskDto))) {
			taskGlobalVariable.put(TaskGlobalVariable.SOURCE_INITIAL_COUNTER_KEY, new AtomicInteger(sourceNodes.size()));
		}
	}

	protected Map<String, TapTableMap<String, TapTable>> engineTransformSchema(TaskDto taskDto) {
		AspectUtils.executeAspect(new EngineDeductionAspect().start());
		ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto);
		Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap = new HashMap<>();
		try {
			com.tapdata.tm.commons.dag.DAG dag = taskDto.getDag().clone();
			TransformerWsMessageDto transformerWsMessageDto = clientMongoOperator.findOne(new Query(),
					ConnectorConstant.TASK_COLLECTION + "/transformAllParam/" + taskDto.getId().toHexString(),
					TransformerWsMessageDto.class);
			if (taskDto.isPreviewTask()) {
				transformerWsMessageDto.getTaskDto().setSyncType(taskDto.getSyncType());
				transformerWsMessageDto.getTaskDto().setTestTaskId(taskDto.getTestTaskId());
				transformerWsMessageDto.getOptions().setSyncType(taskDto.getSyncType());
			}
			transformerWsMessageDto.getTaskDto().setDag(dag);
			DAGDataServiceImpl dagDataService = new DAGDataEngineServiceImpl(transformerWsMessageDto, taskService, tapTableMapHashMap, clientMongoOperator);
			dag.transformSchema(null, dagDataService, transformerWsMessageDto.getOptions(), (e) -> {
				throw new RuntimeException(e);
			});
			dagDataService.initializeModel((StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(), TaskDto.SYNC_TYPE_SYNC)));
			AspectUtils.executeAspect(new EngineDeductionAspect().end());
			obsLogger.info("Loading table structure completed");
		} catch (Exception e) {
			AspectUtils.executeAspect(new EngineDeductionAspect().error(e));
			obsLogger.info("Loading table structure error: {}", e.getMessage());
			throw new TapCodeException(TaskServiceExCode_23.TASK_FAILED_TO_LOAD_TABLE_STRUCTURE, "reason:" + e.getMessage(), e)
					.dynamicDescriptionParameters(taskDto.getName(), taskDto.getId(), taskDto.getSyncType());
		}
		return tapTableMapHashMap;
	}

	protected void cleanMergeNode(TaskDto taskDto, String nodeId){
		if (!taskDto.isNormalTask() || taskDto.isCDCTask() || taskDto.hasSyncProgress() || taskDto.hasDisableNode()) {
			return;
		}

		MergeNodeCleaner mergeNodeCleaner = new MergeNodeCleaner();
		mergeNodeCleaner.cleanTaskNode(taskDto.getId().toHexString(), nodeId);
		logger.info("Clear {} master-slave merge cache", nodeId);
	}

	protected void initEnv(TaskDto taskDto, HazelcastInstance hazelcastInstance) {
		if (null == taskDto) {
			return;
		}
		Map<String, String> env = taskDto.getEnv();
		TaskEnvMap taskEnvMap = new TaskEnvMap();
		if (MapUtils.isNotEmpty(env)) {
			taskEnvMap.putAll(env);
		}
		String taskId = taskDto.getId().toHexString();
		PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);
		globalStateMap.put(TaskEnvMap.name(taskId), taskEnvMap);
	}
}

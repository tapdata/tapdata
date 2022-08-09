package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.tapdata.cache.ICacheService;
import com.tapdata.cache.hazelcast.HazelcastCacheService;
import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.DataNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.SettingService;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.dao.MessageDao;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.common.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.*;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastCustomProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastMergeNode;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorNode;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation.HazelcastMultiAggregatorProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.MergeTableUtil;
import io.tapdata.flow.engine.V2.util.NodeUtil;
import io.tapdata.milestone.MilestoneContext;
import io.tapdata.milestone.MilestoneFactory;
import io.tapdata.milestone.MilestoneFlowServiceJetV2;
import io.tapdata.milestone.MilestoneJetEdgeService;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author jackin
 * @date 2021/12/1 8:56 PM
 **/
@Service
@DependsOn("tapdataTaskScheduler")
public class HazelcastTaskService implements TaskService<SubTaskDto> {

	private static final Logger logger = LogManager.getLogger(HazelcastTaskService.class);

	private static HazelcastInstance hazelcastInstance;

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
	}

	@PostConstruct
	public void init() {
		String agentId = (String) configurationCenter.getConfig(ConfigurationCenter.AGENT_ID);
		Config config = HazelcastUtil.getConfig(agentId);
		ShareCdcUtil.initHazelcastPersistenceStorage(config, settingService, clientMongoOperator);
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		cacheService = new HazelcastCacheService(hazelcastInstance, clientMongoOperator);
		messageDao.setCacheService(cacheService);
	}

	public static HazelcastInstance getHazelcastInstance() {
		return hazelcastInstance;
	}

	@Override
	public TaskClient<SubTaskDto> startTask(SubTaskDto subTaskDto) {
		final JetDag jetDag = task2HazelcastDAG(subTaskDto);
		MilestoneFlowServiceJetV2 milestoneFlowServiceJetV2 = initMilestone(subTaskDto);

		JobConfig jobConfig = new JobConfig();
		jobConfig.setName(subTaskDto.getName() + "-" + subTaskDto.getId().toHexString());
		jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
		AspectUtils.executeAspect(new TaskStartAspect().task(subTaskDto));
		final Job job = hazelcastInstance.getJet().newJob(jetDag.getDag(), jobConfig);
		return new HazelcastTaskClient(job, subTaskDto, clientMongoOperator, configurationCenter, hazelcastInstance, milestoneFlowServiceJetV2);
	}

	@Override
	public TaskClient<SubTaskDto> startTestTask(SubTaskDto subTaskDto) {
		long startTs = System.currentTimeMillis();
		final JetDag jetDag = task2HazelcastDAG(subTaskDto);
		JobConfig jobConfig = new JobConfig();
		jobConfig.setProcessingGuarantee(ProcessingGuarantee.NONE);
		AspectUtils.executeAspect(new TaskStartAspect().task(subTaskDto));
		logger.info("task2HazelcastDAG cost {}ms", (System.currentTimeMillis() - startTs));

		Job job = hazelcastInstance.getJet().newLightJob(jetDag.getDag(), jobConfig);
		return new HazelcastTaskClient(job, subTaskDto, clientMongoOperator, configurationCenter, hazelcastInstance, null);
	}

	@SneakyThrows
	private JetDag task2HazelcastDAG(SubTaskDto subTaskDto) {

		DAG dag = new DAG();
		AtomicReference<SubTaskDto> subTaskDtoAtomicReference = new AtomicReference<>(subTaskDto);

		Long tmCurrentTime = subTaskDtoAtomicReference.get().getTmCurrentTime();
		if (null != tmCurrentTime && tmCurrentTime.compareTo(0L) > 0) {
			Map<String, Object> params = new HashMap<>();
			params.put("id", subTaskDto.getId().toHexString());
			params.put("time", tmCurrentTime);
			clientMongoOperator.deleteByMap(params, ConnectorConstant.SUB_TASK_COLLECTION + "/history");
			subTaskDtoAtomicReference.set(clientMongoOperator.findOne(params, ConnectorConstant.SUB_TASK_COLLECTION + "/history", SubTaskDto.class));
			if (null == subTaskDtoAtomicReference.get()) {
				throw new RuntimeException("Get task history failed, param: " + params + ", result is null");
			}
		}

		final List<Node> nodes = subTaskDtoAtomicReference.get().getDag().getNodes();
		final List<Edge> edges = subTaskDtoAtomicReference.get().getDag().getEdges();
		Map<String, Vertex> vertexMap = new HashMap<>();
		Map<String, AbstractProcessor> hazelcastBaseNodeMap = new HashMap<>();
		Map<String, AbstractProcessor> typeConvertMap = new HashMap<>();
		Map<String, Node<?>> nodeMap = nodes.stream().collect(Collectors.toMap(Element::getId, n -> n));

		final ConfigurationCenter config = (ConfigurationCenter) configurationCenter.clone();
		if (CollectionUtils.isNotEmpty(nodes)) {

			// Get merge table map
			Map<String, MergeTableNode> mergeTableMap = MergeTableUtil.getMergeTableMap(nodes, edges);

			for (Node node : nodes) {
				Connections connection = null;
				DatabaseTypeEnum.DatabaseType databaseType = null;
				TapTableMap<String, TapTable> tapTableMap = getTapTableMap(subTaskDto, tmCurrentTime, node);
				if (CollectionUtils.isEmpty(tapTableMap.keySet())
						&& !(node instanceof CacheNode)
						&& !(node instanceof HazelCastImdgNode)
						&& !(node instanceof TableRenameProcessNode)
						&& !(node instanceof MigrateFieldRenameProcessorNode)
						&& !(node instanceof VirtualTargetNode)
						&& !StringUtils.equalsAnyIgnoreCase(subTaskDto.getParentTask().getSyncType(), TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
					throw new NodeException(String.format("Node [id %s, name %s] schema cannot be empty",
							node.getId(), node.getName()));
				}

				if (node instanceof TableNode || node instanceof DatabaseNode || node.isLogCollectorNode()) {
					String connectionId = null;
					if (node instanceof DataNode) {
						connectionId = ((DataNode) node).getConnectionId();
					} else if (node instanceof DatabaseNode) {
						connectionId = ((DatabaseNode) node).getConnectionId();
					} else if (node instanceof LogCollectorNode) {
						connectionId = ((LogCollectorNode) node).getConnectionIds().get(0);
					}
					connection = getConnection(connectionId);
					databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());

//					if ("pdk".equals(connection.getPdkType())) {
//						PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator, databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
//					}
				} else if (node instanceof CacheNode) {
					Optional<Edge> edge = edges.stream().filter(e -> e.getTarget().equals(node.getId())).findFirst();
					Node sourceNode = null;
					if (edge.isPresent()) {
						sourceNode = nodeMap.get(edge.get().getSource());
						if (sourceNode instanceof TableNode) {
							connection = getConnection(((TableNode) sourceNode).getConnectionId());
						}
					}
					messageDao.registerCache((CacheNode) node, (TableNode) sourceNode, connection, subTaskDtoAtomicReference.get(), clientMongoOperator);
				}
				List<Node> predecessors = node.predecessors();
				List<Node> successors = node.successors();
				Connections finalConnection = connection;
				DatabaseTypeEnum.DatabaseType finalDatabaseType = databaseType;

				TapTableMap<String, TapTable> finalTapTableMap = tapTableMap;
				Vertex vertex = new Vertex(NodeUtil.getVertexName(node), () -> {
					try {
						Log4jUtil.setThreadContext(subTaskDtoAtomicReference.get());
						return createNode(
								subTaskDtoAtomicReference.get(),
								nodes,
								edges,
								node,
								predecessors,
								successors,
								config,
								finalConnection,
								finalDatabaseType,
								mergeTableMap,
								finalTapTableMap
						);
					} catch (Exception e) {
						logger.error("create dag node failed: {}", e.getMessage(), e);
						throw e;
					}
				});
				vertexMap.put(node.getId(), vertex);

				vertex.localParallelism(1);
				dag.vertex(vertex);
			}

			handleEdge(dag, edges, nodeMap, vertexMap);
		}

		return new JetDag(dag, hazelcastBaseNodeMap, typeConvertMap);
	}

	private static TapTableMap<String, TapTable> getTapTableMap(SubTaskDto subTaskDto, Long tmCurrentTime, Node node) {
		TapTableMap<String, TapTable> tapTableMap;
		if (StringUtils.equalsAnyIgnoreCase(subTaskDto.getParentTask().getSyncType(),
//						TaskDto.SYNC_TYPE_TEST_RUN,
						TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
			tapTableMap = TapTableUtil.getTapTableMap(node, tmCurrentTime);
		} else {
			tapTableMap = TapTableUtil.getTapTableMapByNodeId(node.getId(), tmCurrentTime);
		}
		return tapTableMap;
	}

	private boolean needForceNodeSchema(SubTaskDto subTaskDto) {
		if (subTaskDto.getParentTask().getSyncType().equals("logCollector")) {
			return false;
		}
		return true;
	}

	public static HazelcastBaseNode createNode(
			SubTaskDto subTaskDto,
			List<Node> nodes,
			List<Edge> edges,
			Node node,
			List<Node> predecessors,
			List<Node> successors,
			ConfigurationCenter config,
			Connections connection,
			DatabaseTypeEnum.DatabaseType databaseType,
			Map<String, MergeTableNode> mergeTableMap,
			TapTableMap<String, TapTable> tapTableMap
	) throws Exception {
		List<RelateDataBaseTable> nodeSchemas = new ArrayList<>();
		HazelcastBaseNode hazelcastNode;
		final String type = node.getType();
		final NodeTypeEnum nodeTypeEnum = NodeTypeEnum.get(type);
		switch (nodeTypeEnum) {
			case DATABASE:
			case TABLE:
				if (CollectionUtils.isNotEmpty(predecessors) && CollectionUtils.isNotEmpty(successors)) {
					if ("pdk".equals(connection.getPdkType())) {
						hazelcastNode = new HazelcastPdkSourceAndTargetTableNode(
								DataProcessorContext.newBuilder()
										.withSubTaskDto(subTaskDto)
										.withNode(node)
										.withNodes(nodes)
										.withEdges(edges)
										.withConfigurationCenter(config)
										.withConnectionConfig(connection.getConfig())
										.withDatabaseType(databaseType)
										.withTapTableMap(tapTableMap)
										.build()
						);
					} else {
						hazelcastNode = new HazelcastTaskSourceAndTarget(
								DataProcessorContext.newBuilder()
										.withSubTaskDto(subTaskDto)
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
								.withSubTaskDto(subTaskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withSourceConn(connection)
								.withConnectionConfig(connection.getConfig())
								.withDatabaseType(databaseType)
								.withTapTableMap(tapTableMap)
								.build();
						if (StringUtils.equalsAnyIgnoreCase(subTaskDto.getParentTask().getSyncType(),
								TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
							hazelcastNode = new HazelcastSampleSourcePdkDataNode(processorContext);
						} else {
							hazelcastNode = new HazelcastSourcePdkDataNode(processorContext);
						}
					} else {
						hazelcastNode = new HazelcastTaskSource(
								DataProcessorContext.newBuilder()
										.withSubTaskDto(subTaskDto)
										.withNode(node)
										.withNodes(nodes)
										.withEdges(edges)
										.withConfigurationCenter(config)
										.withSourceConn(connection)
										.build());
					}
				} else {
					if ("pdk".equals(connection.getPdkType())) {
						hazelcastNode = new HazelcastTargetPdkDataNode(
								DataProcessorContext.newBuilder()
										.withSubTaskDto(subTaskDto)
										.withNode(node)
										.withNodes(nodes)
										.withEdges(edges)
										.withConfigurationCenter(config)
										.withConnectionConfig(connection.getConfig())
										.withDatabaseType(databaseType)
										.withTapTableMap(tapTableMap)
										.build());
					} else {
						hazelcastNode = new HazelcastTaskTarget(
								DataProcessorContext.newBuilder()
										.withSubTaskDto(subTaskDto)
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
									.withSubTaskDto(subTaskDto)
									.withNode(node)
									.withNodes(nodes)
									.withEdges(edges)
									.withConfigurationCenter(config)
									.withConnectionConfig(connection.getConfig())
									.withDatabaseType(databaseType)
									.withTapTableMap(tapTableMap)
									.withCacheService(cacheService)
									.build()
					);
				} else {
					hazelcastNode = new HazelcastCacheTarget(
							DataProcessorContext.newBuilder()
									.withSubTaskDto(subTaskDto)
									.withNode(node)
									.withNodes(nodes)
									.withEdges(edges)
									.withConfigurationCenter(config)
									.withTargetConn(connection)
									.withCacheService(cacheService)
									.withTapTableMap(tapTableMap)
									.build()
					);
				}
				break;
			case VIRTUAL_TARGET:
				DataProcessorContext processorContext = DataProcessorContext.newBuilder()
						.withSubTaskDto(subTaskDto)
						.withNode(node)
						.withNodes(nodes)
						.withEdges(edges)
						.withConfigurationCenter(config)
						.withTargetConn(connection)
						.withCacheService(cacheService)
						.withTapTableMap(tapTableMap)
						.build();
				if (TaskDto.SYNC_TYPE_TEST_RUN.equals(subTaskDto.getParentTask().getSyncType())) {
					hazelcastNode = new HazelcastVirtualTargetNode(processorContext);
				} else {
					hazelcastNode = new HazelcastSchemaTargetNode(processorContext);
				}

				break;
			case JOIN:
				hazelcastNode = new HazelcastJoinProcessor(
						ProcessorBaseContext.newBuilder()
								.withSubTaskDto(subTaskDto)
								.withNode(node)
								.withNodeSchemas(nodeSchemas)
								.withTapTableMap(tapTableMap)
								.build()
				);
				break;
			case JS_PROCESSOR:
			case MIGRATE_JS_PROCESSOR:
			case FIELD_PROCESSOR:
			case ROW_FILTER_PROCESSOR:
			case CACHE_LOOKUP_PROCESSOR:
			case FIELD_RENAME_PROCESSOR:
			case FIELD_MOD_TYPE_PROCESSOR:
			case FIELD_CALC_PROCESSOR:
			case TABLE_RENAME_PROCESSOR:
			case MIGRATE_FIELD_RENAME_PROCESSOR:
			case FIELD_ADD_DEL_PROCESSOR:
				hazelcastNode = new HazelcastProcessorNode(
						DataProcessorContext.newBuilder()
								.withSubTaskDto(subTaskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withCacheService(cacheService)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.build()
				);
				break;
			case LOG_COLLECTOR:
				hazelcastNode = new HazelcastSourcePdkShareCDCNode(
						DataProcessorContext.newBuilder()
								.withSubTaskDto(subTaskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.withConnectionConfig(connection.getConfig())
								.withDatabaseType(databaseType)
								.build()
				);
				break;
			case HAZELCASTIMDG:
				Connections connections = new Connections();
				connections.setDatabase_type(DatabaseTypeEnum.HAZELCAST_IMDG.getType());
				hazelcastNode = new HazelcastTargetPdkShareCDCNode(
						DataProcessorContext.newBuilder()
								.withSubTaskDto(subTaskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.build()
				);
				break;
			case CUSTOM_PROCESSOR:
				hazelcastNode = new HazelcastCustomProcessor(
						DataProcessorContext.newBuilder()
								.withSubTaskDto(subTaskDto)
								.withNode(node)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.build()
				);
				break;
			case MERGETABLE:
				hazelcastNode = new HazelcastMergeNode(
						DataProcessorContext.newBuilder()
								.withSubTaskDto(subTaskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.build()
				);
				break;
			case AGGREGATION_PROCESSOR:
				hazelcastNode = new HazelcastMultiAggregatorProcessor(
						DataProcessorContext.newBuilder()
								.withSubTaskDto(subTaskDto)
								.withNode(node)
								.withNodes(nodes)
								.withEdges(edges)
								.withConfigurationCenter(config)
								.withTapTableMap(tapTableMap)
								.build()
				);
				break;
			default:
				hazelcastNode = new HazelcastBlank(
						DataProcessorContext.newBuilder()
								.withSubTaskDto(subTaskDto)
								.withNode(node)
								.build()
				);
				break;
		}
		MergeTableUtil.setMergeTableIntoHZTarget(mergeTableMap, hazelcastNode);
		return hazelcastNode;
	}

	private boolean needAddTypeConverter(SubTaskDto subTaskDto) {
		if (subTaskDto.getParentTask().getSyncType().equals("logCollector")) {
			return false;
		}
		return true;
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
				dag.edge(
						com.hazelcast.jet.core.Edge
								.from(vertexMap.get(source), outboundEdges.size())
								.to(vertexMap.get(target), inboundEdges.size())
				);
			}
		}
	}

	@Override
	public TaskClient getTaskClient(String taskId) {
		return null;
	}

	@Override
	public List<TaskClient<SubTaskDto>> getTaskClients() {
		return null;
	}

	private Connections getConnection(String connectionId) {
		final Connections connections = clientMongoOperator.findOne(
				new Query(where("_id").is(connectionId)),
				ConnectorConstant.CONNECTION_COLLECTION,
				Connections.class
		);
		if (null == connections) {
			throw new RuntimeException("Cannot find connection by id(" + connectionId + ")");
		}
		connections.decodeDatabasePassword();
		connections.initCustomTimeZone();
		return connections;
	}

	private MilestoneFlowServiceJetV2 initMilestone(SubTaskDto subTaskDto) {
		if (null == subTaskDto) {
			throw new IllegalArgumentException("Input parameter subTaskDto,dag cannot be empty");
		}

		// 初始化dag里面每条连线的里程碑
		List<Node> nodes = subTaskDto.getDag().getNodes();
		HttpClientMongoOperator httpClientMongoOperator = (HttpClientMongoOperator) clientMongoOperator;

		MilestoneFlowServiceJetV2 jetMilestoneService = MilestoneFactory.getJetMilestoneService(subTaskDto, httpClientMongoOperator.getRestTemplateOperator().getBaseURLs(),
				httpClientMongoOperator.getRestTemplateOperator().getRetryTime(), httpClientMongoOperator.getConfigCenter());

		List<Node> dataNodes = nodes.stream().filter(n -> n.isDataNode() || n instanceof DatabaseNode).collect(Collectors.toList());

		for (Node<?> node : dataNodes) {
			String sourceVertexName = NodeUtil.getVertexName(node);
			List<Node<?>> successors = GraphUtil.successors(node, Node::isDataNode);
			for (Node<?> successor : successors) {
				String destVertexName = NodeUtil.getVertexName(successor);
				MilestoneContext taskMilestoneContext = jetMilestoneService.getMilestoneContext();
				MilestoneJetEdgeService jetEdgeMilestoneService = MilestoneFactory.getJetEdgeMilestoneService(
						subTaskDto,
						httpClientMongoOperator.getRestTemplateOperator().getBaseURLs(),
						httpClientMongoOperator.getRestTemplateOperator().getRetryTime(),
						httpClientMongoOperator.getConfigCenter(),
						node,
						successor,
						sourceVertexName,
						destVertexName,
						taskMilestoneContext
				);

				List<Milestone> milestones = jetEdgeMilestoneService.initMilestones();
				jetEdgeMilestoneService.updateList(milestones);
			}
		}

		// 初始化并更新整个SubTask的里程碑
		jetMilestoneService.updateList();

		return jetMilestoneService;
	}
}

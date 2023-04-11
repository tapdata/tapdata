package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
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
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.JetDag;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.exception.AutoInspectException;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.AutoInspectNode;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.autoinspect.utils.AutoInspectNodeUtil;
import io.tapdata.common.SettingService;
import io.tapdata.dao.MessageDao;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.entity.GlobalConstant;
import io.tapdata.flow.engine.V2.exception.TaskSchedulerExCode_12;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastBlank;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastCacheTarget;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastSchemaTargetNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastTaskSource;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastTaskSourceAndTarget;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastTaskTarget;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastVirtualTargetNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation.HazelcastMultiAggregatorProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.MergeTableUtil;
import io.tapdata.flow.engine.V2.util.NodeUtil;
import io.tapdata.observable.logging.ObsLoggerFactory;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author jackin
 * @date 2021/12/1 8:56 PM
 **/
@Service
@DependsOn("tapdataTaskScheduler")
public class HazelcastTaskService implements TaskService<TaskDto> {

    private static final Logger logger = LogManager.getLogger(HazelcastTaskService.class);
    private static final String TAG = HazelcastTaskService.class.getSimpleName();

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
//        TaskThreadGroup threadGroup = new TaskThreadGroup(taskDto);
//        try (ThreadPoolExecutorEx threadPoolExecutorEx = AsyncUtils.createThreadPoolExecutor("RootTask-" + taskDto.getName(), 1, threadGroup, TAG)) {
        try {
            AspectUtils.executeAspect(new TaskStartAspect().task(taskDto));
//            return threadPoolExecutorEx.submitSync(() -> {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(taskDto.getName() + "-" + taskDto.getId().toHexString());
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
            JetService jet = hazelcastInstance.getJet();
            final JetDag jetDag = task2HazelcastDAG(taskDto);
            ObsLoggerFactory.getInstance().getObsLogger(taskDto).info("The engine receives " + taskDto.getName() + " task data from TM and will continue to run tasks by jet");
            Job job = jet.newJob(jetDag.getDag(), jobConfig);
            return new HazelcastTaskClient(job, taskDto, clientMongoOperator, configurationCenter, hazelcastInstance);
//            });
        } catch (Throwable throwable) {
            ObsLoggerFactory.getInstance().getObsLogger(taskDto).error(throwable);
            AspectUtils.executeAspect(new TaskStopAspect().task(taskDto).error(throwable));
            throw throwable;
        }
    }

    @Override
    public TaskClient<TaskDto> startTestTask(TaskDto taskDto) {
        try {
            AspectUtils.executeAspect(new TaskStartAspect().task(taskDto));
            long startTs = System.currentTimeMillis();
            final JetDag jetDag = task2HazelcastDAG(taskDto);
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

    @SneakyThrows
    private JetDag task2HazelcastDAG(TaskDto taskDto) {

        DAG dag = new DAG();
        AtomicReference<TaskDto> taskDtoAtomicReference = new AtomicReference<>(taskDto);
        TaskConfig taskConfig = getTaskConfig(taskDto);

        Long tmCurrentTime = taskDtoAtomicReference.get().getTmCurrentTime();
        if (null != tmCurrentTime && tmCurrentTime.compareTo(0L) > 0) {
            Map<String, Object> params = new HashMap<>();
            params.put("id", taskDto.getId().toHexString());
            params.put("time", tmCurrentTime);
            clientMongoOperator.deleteByMap(params, ConnectorConstant.TASK_COLLECTION + "/history");
            taskDtoAtomicReference.set(clientMongoOperator.findOne(params, ConnectorConstant.TASK_COLLECTION + "/history", TaskDto.class));
            if (null == taskDtoAtomicReference.get()) {
                throw new RuntimeException("Get task history failed, param: " + params + ", result is null");
            }
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
                    nodes.add(inspectNode);
                    nodeMap.put(inspectNode.getId(), inspectNode);
                    edges.add(new Edge(inspectNode.getFromNode().getId(), inspectNode.getId()));
                }
            } catch (AutoInspectException e) {
                ObsLoggerFactory.getInstance().getObsLogger(taskDtoAtomicReference.get()).warn(e.getMessage());
            }

            for (Node node : nodes) {
                Connections connection = null;
                DatabaseTypeEnum.DatabaseType databaseType = null;
                TapTableMap<String, TapTable> tapTableMap = getTapTableMap(taskDto, tmCurrentTime, node);
                if (CollectionUtils.isEmpty(tapTableMap.keySet())
                        && !(node instanceof AutoInspectNode)
                        && !(node instanceof CacheNode)
                        && !(node instanceof HazelCastImdgNode)
                        && !(node instanceof TableRenameProcessNode)
                        && !(node instanceof MigrateFieldRenameProcessorNode)
                        && !(node instanceof VirtualTargetNode)
                        && !StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(), TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)
                ) {
                    throw new NodeException(String.format("Node [id %s, name %s] schema cannot be empty",
                            node.getId(), node.getName()));
                }

                if (node instanceof DataParentNode) {
                    connection = getConnection(((DataParentNode<?>) node).getConnectionId());
                    databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());
                } else if (node.isLogCollectorNode()) {
                    String connectionId = ((LogCollectorNode) node).getConnectionIds().get(0);
                    connection = getConnection(connectionId);
                    databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());
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
                }
                List<Node> predecessors = node.predecessors();
                List<Node> successors = node.successors();
                Connections finalConnection = connection;
                DatabaseTypeEnum.DatabaseType finalDatabaseType = databaseType;
                TapTableMap<String, TapTable> finalTapTableMap = tapTableMap;
                Vertex vertex = new Vertex(NodeUtil.getVertexName(node), () -> {
                    try {
                        return createNode(
                                taskDtoAtomicReference.get(),
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
                    } catch (Exception e) {
                        throw new TapCodeException(TaskProcessorExCode_11.CREATE_PROCESSOR_FAILED,
                                String.format("Failed to create processor based on node information, node: %s[%s]", node.getName(), node.getId()), e);
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

    private static TapTableMap<String, TapTable> getTapTableMap(TaskDto taskDto, Long tmCurrentTime, Node node) {
        TapTableMap<String, TapTable> tapTableMap;
        if (StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(),
//						TaskDto.SYNC_TYPE_TEST_RUN,
                TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
            tapTableMap = TapTableUtil.getTapTableMap(node, tmCurrentTime);
        } else {
            tapTableMap = TapTableUtil.getTapTableMapByNodeId(node.getId(), tmCurrentTime);
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

							if(readPartitionOptions != null && readPartitionOptions.isEnable() && readPartitionOptions.getSplitType() != ReadPartitionOptions.SPLIT_TYPE_NONE) {
								hazelcastNode = new HazelcastSourcePartitionReadDataNode(processorContext);
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
						hazelcastNode = new HazelcastTargetPdkDataNode(
								DataProcessorContext.newBuilder()
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
										.build());
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
                hazelcastNode = new HazelcastMergeNode(
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
            case AGGREGATION_PROCESSOR:
                hazelcastNode = new HazelcastMultiAggregatorProcessor(
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
    public List<TaskClient<TaskDto>> getTaskClients() {
        return null;
    }

    public Connections getConnection(String connectionId) {
        return getConnection(connectionId, null);
    }

    private Connections getConnection(String connectionId, List<String> includeFields) {
        Query query = new Query(where("_id").is(connectionId));
        if (CollectionUtils.isNotEmpty(includeFields)) {
            for (String includeField : includeFields) {
                query.fields().include(includeField);
            }
        }
        final Connections connections = clientMongoOperator.findOne(
                query,
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

    private TaskConfig getTaskConfig(TaskDto taskDto) {
        return TaskConfig.create()
                .taskDto(taskDto)
                .taskRetryConfig(getTaskRetryConfig())
                .externalStorageDtoMap(ExternalStorageUtil.getExternalStorageMap(taskDto, clientMongoOperator));
    }

    private TaskRetryConfig getTaskRetryConfig() {
        long retryIntervalSecond = settingService.getLong("retry_interval_second", 60L);
        long maxRetryTimeMinute = settingService.getLong("max_retry_time_minute", 60L);
        long maxRetryTimeSecond = maxRetryTimeMinute * 60;
        return TaskRetryConfig.create()
                .retryIntervalSecond(retryIntervalSecond)
                .maxRetryTimeSecond(maxRetryTimeSecond);
    }
}

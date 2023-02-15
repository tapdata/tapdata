package com.tapdata.tm.task.service;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldChangeRuleGroup;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.transform.service.MetadataTransformerItemService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.utils.GZIPUtil;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2021/12/17
 * @Description:
 */
@Service
@Slf4j
public class TransformSchemaService {

    private final DAGDataService dagDataService;
    private final MetadataInstancesService metadataInstancesService;
    private final TaskService taskService;
    private final MetadataTransformerService metadataTransformerService;
    private final MetadataTransformerItemService metadataTransformerItemService;

    private final DataSourceService dataSourceService;

    private final DataSourceDefinitionService definitionService;

    private MessageQueueService messageQueueService;
    private WorkerService workerService;
    private TaskDagCheckLogService taskDagCheckLogService;

    @Autowired
    public TransformSchemaService(DAGDataService dagDataService, MetadataInstancesService metadataInstancesService, TaskService taskService,
                                  DataSourceService dataSourceService, MetadataTransformerService metadataTransformerService,
                                  DataSourceDefinitionService definitionService, MetadataTransformerItemService metadataTransformerItemService,
                                  MessageQueueService messageQueueService, WorkerService workerService, TaskDagCheckLogService taskDagCheckLogService) {
        this.dagDataService = dagDataService;
        this.metadataInstancesService = metadataInstancesService;
        this.taskService = taskService;
        this.metadataTransformerService = metadataTransformerService;
        this.dataSourceService = dataSourceService;
        this.definitionService = definitionService;
        this.metadataTransformerItemService = metadataTransformerItemService;
        this.messageQueueService = messageQueueService;
        this.workerService = workerService;
        this.taskDagCheckLogService = taskDagCheckLogService;
    }

    @Value("${tm.transform.batch.num:1000}")
    private int transformBatchNum;

    public void transformSchema(DAG dag, UserDetail user, ObjectId taskId) {
        TaskDto taskDto = taskService.checkExistById(taskId, user);
        taskDto.setDag(dag);
        try {
            transformSchema(taskDto, user);
        } catch (Exception e) {
            taskDagCheckLogService.createLog(taskId.toHexString(), user.getUserId(), Level.ERROR,
                    DagOutputTemplateEnum.MODEL_PROCESS_CHECK,
                    false, true, DateUtil.now(), e.getMessage());
            taskService.update(new Query(Criteria.where("_id").is(taskId)), Update.update("transformDagHash", 0));
        }
    }

    public TransformerWsMessageDto getTransformParam(TaskDto taskDto, UserDetail user) {
        return getTransformParam(taskDto, user, false);
    }
    public TransformerWsMessageDto getTransformParam(TaskDto taskDto, UserDetail user, boolean allParam) {
        log.debug("start transform schema, task = {}, user = {}", taskDto, user);

        taskDto.setUserId(user.getUserId());
        DAG dag = taskDto.getDag();

        Map<String, List<SchemaTransformerResult>> results = new HashMap<>();
        Map<String, List<SchemaTransformerResult>> lastBatchResults = new HashMap<>();

        dag.addNodeEventListener(new Node.EventListener<Object>() {
            @Override
            public void onTransfer(List<Object> inputSchemaList, Object schema, Object outputSchema, String nodeId) {
                List<SchemaTransformerResult> schemaTransformerResults = results.get(nodeId);
                if (schemaTransformerResults == null) {
                    return;
                }
                List<Schema> outputSchemaList;
                if (outputSchema instanceof List) {
                    outputSchemaList = (List) outputSchema;

                } else {
                    Schema outputSchema1 = (Schema) outputSchema;
                    outputSchemaList = Lists.newArrayList(outputSchema1);
                }

                List<String> sourceQualifiedNames = outputSchemaList.stream().map(Schema::getQualifiedName).collect(Collectors.toList());
                Criteria criteria = Criteria.where("qualified_name").in(sourceQualifiedNames);
                Query query = new Query(criteria);
                query.fields().include("_id", "qualified_name");
                List<MetadataInstancesEntity> all = metadataInstancesService.findAll(query, user);
                Map<String, MetadataInstancesEntity> metaMaps = all.stream().collect(Collectors.toMap(MetadataInstancesEntity::getQualifiedName, m -> m, (m1, m2) -> m1));
                for (SchemaTransformerResult schemaTransformerResult : schemaTransformerResults) {
                    if (Objects.isNull(schemaTransformerResult)) {
                        continue;
                    }
                    MetadataInstancesEntity metadataInstancesEntity = metaMaps.get(schemaTransformerResult.getSinkQulifiedName());
                    if (metadataInstancesEntity != null && metadataInstancesEntity.getId() != null) {
                        schemaTransformerResult.setSinkTableId(metadataInstancesEntity.getId().toHexString());
                    }
                }
            }

            @Override
            public void schemaTransformResult(String nodeId, Node node, List<SchemaTransformerResult> schemaTransformerResults) {
                List<SchemaTransformerResult> results1 = results.get(nodeId);
                if (CollectionUtils.isNotEmpty(results1)) {
                    results1.addAll(schemaTransformerResults);
                } else {
                    results.put(nodeId, schemaTransformerResults);
                }
                lastBatchResults.put(nodeId, schemaTransformerResults);
            }

            @Override
            public List<SchemaTransformerResult> getSchemaTransformResult(String nodeId) {
                return lastBatchResults.get(nodeId);
            }
        });

        DAG.Options options = new DAG.Options(taskDto.getRollback(), taskDto.getRollbackTable());
        options.setSyncType(taskDto.getSyncType());
        options.setBatchNum(transformBatchNum);
        if (StringUtils.isBlank(options.getUuid())) {
            //考虑到先后问题，采用毫秒级时间戳更好一点
            options.setUuid(String.valueOf(System.currentTimeMillis()));
        }
        // update metaTransformer version
        dag.getTargets().forEach(target -> metadataTransformerService.updateVersion(taskDto.getId().toHexString(), target.getId(), options.getUuid()));

        List<Node> dagNodes = dag.getNodes();
        dagNodes.forEach(node -> {
            node.setService(dagDataService);
            node.getDag().setTaskId(taskDto.getId());

            if (node instanceof DataParentNode) {
                Optional.ofNullable(((DataParentNode<?>) node).getFieldChangeRules()).ifPresent(fieldChangeRules -> {
                    if (null == options.getFieldChangeRules()) {
                        options.setFieldChangeRules(new FieldChangeRuleGroup());
                    }
                    options.getFieldChangeRules().addAll(node.getId(), fieldChangeRules);
                });
            }
        });
        List<Node> nodes = dagNodes;

        List<MetadataInstancesDto> metadataList = new ArrayList<>();
        Map<String, DataSourceConnectionDto> dataSourceMap = new HashMap<>();
        Map<String, DataSourceDefinitionDto> definitionDtoMap = new HashMap<>();
        Map<String, TaskDto> taskMap = new HashMap<>();

        List<String> connectionIds = nodes.stream().filter(n -> n instanceof DataParentNode).map(n -> ((DataParentNode) n).getConnectionId()).collect(Collectors.toList());
        Criteria idCriteria = Criteria.where("_id").in(connectionIds);
        Query query = new Query(idCriteria);
        //TODO query 需要限制好参数
        List<DataSourceConnectionDto> dataSources = dataSourceService.findAllDto(query, user);


        if (CollectionUtils.isNotEmpty(dataSources)) {
            dataSourceMap = dataSources.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), d -> d, (d1, d2) -> d1));
            Set<String> dbTypes = dataSources.stream().map(DataSourceConnectionDto::getDatabase_type).collect(Collectors.toSet());
            List<DataSourceDefinitionDto> definitionDtos = definitionService.getByDataSourceType(new ArrayList<>(dbTypes), user);
            if (CollectionUtils.isNotEmpty(definitionDtos)) {
                definitionDtoMap = definitionDtos.stream().collect(Collectors.toMap(DataSourceDefinitionDto::getType, d -> d, (d1, d2) -> d1));
            }
        }

        final List<String> fileSource = Lists.newArrayList("xml", "json", "excel", "csv");
        if (!allParam) {
            List<String> qualifiedNames = new ArrayList<>();
            for (Node node : nodes) {
                if (node instanceof TableNode) {
                    String connectionId = ((TableNode) node).getConnectionId();
                    DataSourceConnectionDto dataSourceConnectionDto = dataSourceMap.get(connectionId);
                    DataSourceDefinitionDto dataSourceDefinitionDto = definitionDtoMap.get(dataSourceConnectionDto.getDatabase_type());
                    String qualifiedName = metadataInstancesService.getQualifiedNameByNodeId(node, user, dataSourceConnectionDto, dataSourceDefinitionDto, taskDto.getId().toHexString());

                    if (fileSource.contains(dataSourceDefinitionDto.getPdkId())) {
                        int i = qualifiedName.lastIndexOf("_");
                        qualifiedName = qualifiedName.substring(0, i);
                    }
                    qualifiedNames.add(qualifiedName);
                } else if (node instanceof DatabaseNode) {
                    String connectionId = ((DatabaseNode) node).getConnectionId();
                    DataSourceConnectionDto dataSourceConnectionDto = dataSourceMap.get(connectionId);
                    DataSourceDefinitionDto dataSourceDefinitionDto = definitionDtoMap.get(dataSourceConnectionDto.getDatabase_type());

                    List<String> metas = metadataInstancesService.findDatabaseNodeQualifiedName(node.getId(), user, taskDto, dataSourceConnectionDto, dataSourceDefinitionDto);
                    if (fileSource.contains(dataSourceDefinitionDto.getPdkId())) {
                        metas = metas.stream().map(q -> {
                            int i = q.lastIndexOf("_");
                            return q.substring(0, i);
                        }).collect(Collectors.toList());
                    }
                    qualifiedNames.addAll(metas);
                }
            }

            if (CollectionUtils.isNotEmpty(qualifiedNames)) {
                //优先获取逻辑表，没有找到的话，取物理表的。
                metadataList = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedNames, user, "histories");
                Map<String, MetadataInstancesDto> qualifiedMap = metadataList.stream().collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, m -> m, (m1, m2) -> m1));
                qualifiedNames.removeAll(qualifiedMap.keySet());
                qualifiedNames = qualifiedNames.stream().map(q -> {
                    int i = q.lastIndexOf("_");
                    return q.substring(0, i);
                }).collect(Collectors.toList());
                List<MetadataInstancesDto> metadataList1 = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedNames, user, "histories");
                for (MetadataInstancesDto metadataInstancesDto : metadataList1) {
                    metadataInstancesDto.setQualifiedName(metadataInstancesDto.getQualifiedName() + "_" + taskDto.getId().toHexString());
                }
                metadataList.addAll(metadataList1);
            }
        } else {
            Criteria criteria = Criteria.where("taskId").is(taskDto.getId().toHexString())
                    .and("is_deleted").ne(true)
                    .and("sourceType").is(SourceTypeEnum.VIRTUAL.name());
            Query query1 = new Query(criteria);
            query1.fields().exclude("histories");
            metadataList = metadataInstancesService.findAllDto(query1, user);

        }


        List<MetadataInstancesDto> databaseSchemes = metadataInstancesService.findDatabaseSchemeNoHistory(connectionIds, user);
        metadataList.addAll(databaseSchemes);

        Criteria criteria = Criteria.where("dataFlowId").is(taskDto.getId().toHexString());
        List<MetadataTransformerDto> metadataTransformerDtos = metadataTransformerService.findAllDto(new Query(criteria), user);
        Map<String, MetadataTransformerDto> metadataTransformerDtoMap = metadataTransformerDtos.stream().collect(Collectors.toMap(m -> m.getDataFlowId() + m.getStageId(), m -> m, (m1, m2) -> m1));


        TransformerWsMessageDto transformerWsMessageDto = new TransformerWsMessageDto(taskDto, options, metadataList, dataSourceMap, definitionDtoMap, user.getUserId()
                , user.getUsername(), metadataTransformerDtoMap, MessageType.TRANSFORMER.getType());
        return transformerWsMessageDto;

    }

    public void transformSchema(TaskDto taskDto, UserDetail user) {
        transformSchema(taskDto, user, true);
    }

    /**
     *
     * @param taskDto
     * @param user
     * @param checkJs 传true为需要检测js节点， false为补救措施，直接走tm推演。
     */
    public void transformSchema(TaskDto taskDto, UserDetail user, boolean checkJs) {
        log.debug("start transform schema, task = {}, user = {}", taskDto, user);
        TransformerWsMessageDto transformParam = getTransformParam(taskDto, user);

        taskService.updateById(taskDto.getId(), Update.update("transformUuid", transformParam.getOptions().getUuid()).set("transformed", false), user);

        boolean taskContainJs = checkTaskContainJs(taskDto);

        if (checkJs) {
            if (taskContainJs) {
                sendTransformer(transformParam, user);
                return;
            }
        }


        DAGDataServiceImpl dagDataService1 = new DAGDataServiceImpl(transformParam);

        long startTime = System.currentTimeMillis();
        Map<String, List<Message>> transformSchema = taskDto.getDag().transformSchema(null, dagDataService1, transformParam.getOptions());
        long endTime = System.currentTimeMillis();
        log.warn("推演花费 ={}毫秒", (endTime-startTime));
        TransformerWsMessageResult transformerWsMessageResult = new TransformerWsMessageResult();
        transformerWsMessageResult.setTransformSchema(transformSchema);
        transformerWsMessageResult.setUpsertTransformer(dagDataService1.getUpsertTransformer());
        transformerWsMessageResult.setBatchInsertMetaDataList(dagDataService1.getBatchInsertMetaDataList());
        transformerWsMessageResult.setUpsertItems(dagDataService1.getUpsertItems());
        transformerWsMessageResult.setBatchMetadataUpdateMap(dagDataService1.getBatchMetadataUpdateMap());
        transformerWsMessageResult.setTaskId(taskDto.getId().toHexString());
        transformerWsMessageResult.setTransformUuid(transformParam.getOptions().getUuid());
        transformerResult(user, transformerWsMessageResult);
    }

    public void transformerResult(UserDetail user, TransformerWsMessageResult result) {
        transformerResult(user, result, false);
    }
    public void transformerResult(UserDetail user, TransformerWsMessageResult result, boolean saveHistory) {

        String taskId = result.getTaskId();
        TaskDto taskDto = taskService.checkExistById(MongoUtils.toObjectId(taskId), user, "transformUuid");

        if (taskDto == null) {
            return;
        }

        if (!saveHistory) {
            String transformUuid = taskDto.getTransformUuid();
            long tv = Long.parseLong(transformUuid);
            long rv = Long.parseLong(result.getTransformUuid());

            if (rv < tv) {
                return;
            }
        }

        Map<String, List<Message>> msgMap = result.getTransformSchema();
        if (MapUtils.isNotEmpty(msgMap)) {
            log.warn("transformerResult msgMap:" + JSON.toJSONString(msgMap));
            // add transformer task log
            List<String> taskIds = Lists.newArrayList();
            taskIds.addAll(msgMap.keySet());
            taskDagCheckLogService.createLog(taskIds.get(0), user.getUserId(), Level.ERROR, DagOutputTemplateEnum.MODEL_PROCESS_CHECK,
                    false, true, DateUtil.now(), msgMap.get(taskIds.get(0)).get(0).getMsg());
            taskService.update(new Query(Criteria.where("_id").is(taskIds.get(0))), Update.update("transformDagHash", 0));
        }

        metadataInstancesService.bulkSave(result.getBatchInsertMetaDataList(), result.getBatchMetadataUpdateMap(), user, saveHistory, result.getTaskId(), result.getTransformUuid());

        List<String> batchRemoveMetaDataList = result.getBatchRemoveMetaDataList();
        List<String> newBatchRemoveMetaDataList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(batchRemoveMetaDataList)) {
            for (String q : batchRemoveMetaDataList) {
                if (!q.endsWith(taskId)) {
                    newBatchRemoveMetaDataList.add(q + "_" + taskId);
                } else {
                    int i = q.lastIndexOf("_");
                    String oldQualifiedName = q.substring(0, i);
                    newBatchRemoveMetaDataList.add(oldQualifiedName);
                }
            }
            batchRemoveMetaDataList.addAll(newBatchRemoveMetaDataList);
            Criteria criteria = Criteria.where("qualified_name").in(batchRemoveMetaDataList);
            Query query = new Query(criteria);
            metadataInstancesService.deleteAll(query, user);
        }

        if (CollectionUtils.isNotEmpty(result.getUpsertItems())) {
            metadataTransformerItemService.bulkUpsert(result.getUpsertItems());
            MetadataTransformerItemDto itemDto = result.getUpsertItems().get(0);
            String dataFlowId = itemDto.getDataFlowId();
            String uuid = itemDto.getUuid();
            Query query1 = new Query(Criteria.where("dataFlowId").is(dataFlowId).and("uuid").ne(uuid));
            metadataTransformerItemService.deleteAll(query1);
        }

        if (CollectionUtils.isNotEmpty(result.getUpsertTransformer())) {
            metadataTransformerService.save(result.getUpsertTransformer(), user);
        }


        if (!saveHistory) {
            if (StringUtils.isNotBlank(result.getTransformUuid()) && StringUtils.isNotBlank(result.getTaskId())) {
                Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(result.getTaskId()))
                        .and("transformUuid").lte(result.getTransformUuid());
                UpdateResult transformed = taskService.update(new Query(criteria),
                        Update.update("transformed", true).set("transformUuid", result.getTransformUuid()), user);
                if (transformed.getModifiedCount() > 0 && CollectionUtils.isNotEmpty(result.getUpsertTransformer())) {
                    int total = result.getUpsertTransformer().get(0).getTotal();
                    int finished = result.getUpsertTransformer().get(0).getFinished();
                    // add transformer task log
                    taskDagCheckLogService.createLog(taskId, user.getUserId(), Level.INFO, DagOutputTemplateEnum.MODEL_PROCESS_CHECK,
                            false, true, DateUtil.now(), finished, total);
                }
            }
        }
    }


    private void sendTransformer(TransformerWsMessageDto wsMessageDto, UserDetail user) {
        TaskDto taskDto = wsMessageDto.getTaskDto();
        if (taskDto == null) {
            return;
        }

        Map<String, DataSourceDefinitionDto> definitionDtoMap = wsMessageDto.getDefinitionDtoMap();
        if (definitionDtoMap != null) {
            definitionDtoMap.forEach((k, v) -> {
                //有些Properties中的字段属性包含了xx.yy.kk,需要过滤掉。不然入库会报错
                v.setProperties(null);
            });
        }


        List<Worker> availableAgent;
        if (org.apache.commons.lang3.StringUtils.isBlank(taskDto.getAccessNodeType())
                && org.apache.commons.lang3.StringUtils.equalsIgnoreCase(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), taskDto.getAccessNodeType())) {
            availableAgent = workerService.findAvailableAgentByAccessNode(user, taskDto.getAccessNodeProcessIdList());
        } else {
            availableAgent = workerService.findAvailableAgent(user);
        }
        if (CollectionUtils.isEmpty(availableAgent)) {
            return;
        }
        HashMap<Object, Object> data = new HashMap<>();
        data.put("type", MessageType.TRANSFORMER.getType());
        String json = JsonUtil.toJsonUseJackson(wsMessageDto);
        byte[] gzip = GZIPUtil.gzip(json.getBytes());
        byte[] encode = Base64.getEncoder().encode(gzip);
        String dataString = new String(encode);
        data.put("data", dataString);

        String processId = availableAgent.get(0).getProcessId();
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(processId);
        queueDto.setData(data);
        queueDto.setType("pipe");

//        log.info("build send test connection websocket context, processId = {}, userId = {}, queueDto = {}", processId, user.getUserId(), queueDto);
        messageQueueService.sendMessage(queueDto);

    }

    public boolean checkTaskContainJs(TaskDto taskDto) {
        DAG dag = taskDto.getDag();
        if (dag != null) {
            List<Node> nodes = dag.getNodes();
            if (CollectionUtils.isNotEmpty(nodes)) {
                for (Node node : nodes) {
                    if (node instanceof JsProcessorNode || node instanceof MigrateJsProcessorNode || node instanceof CustomProcessorNode) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}

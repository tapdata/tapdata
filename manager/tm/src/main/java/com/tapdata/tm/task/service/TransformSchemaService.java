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
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.lock.annotation.Lock;
import com.tapdata.tm.lock.constant.LockType;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.transform.service.MetadataTransformerItemService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.UUIDUtil;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.checkerframework.checker.nullness.qual.Nullable;
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


    @Lock(value = "taskId", type = LockType.TRANSFORM_SCHEMA)
    public Map<String, List<Message>> transformSchemaSync(DAG dag, UserDetail user, ObjectId taskId) {
        return transformSchema(dag, user, taskId);
    }

    public Map<String, List<Message>> transformSchema(DAG dag, UserDetail user, ObjectId taskId) {
        TaskDto taskDto = taskService.checkExistById(taskId, user);
        taskDto.setDag(dag);
        return transformSchema(taskDto, user);
    }

    public TransformerWsMessageDto getTransformParam(TaskDto taskDto, UserDetail user) {
        log.debug("start transform schema, task = {}, user = {}", taskDto, user);
        taskDto.setUserId(user.getUserId());
        DAG dag = taskDto.getDag();
        List<Node> dagNodes = dag.getNodes();
        dagNodes.forEach(node -> {
            node.setService(dagDataService);
            node.getDag().setTaskId(taskDto.getId());
        });

        DAG.Options options = new DAG.Options(taskDto.getRollback(), taskDto.getRollbackTable());
        options.setSyncType(taskDto.getSyncType());
        options.setBatchNum(transformBatchNum);
        if (StringUtils.isBlank(options.getUuid())) {
            options.setUuid(UUIDUtil.getUUID());
        }
        // update metaTransformer version
        dag.getTargets().forEach(target -> metadataTransformerService.updateVersion(taskDto.getId().toHexString(), target.getId(), options.getUuid()));

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

        List<String> qualifiedNames = new ArrayList<>();
        for (Node node : nodes) {
            if (node instanceof TableNode) {
                String connectionId = ((TableNode) node).getConnectionId();
                DataSourceConnectionDto dataSourceConnectionDto = dataSourceMap.get(connectionId);
                DataSourceDefinitionDto dataSourceDefinitionDto = definitionDtoMap.get(dataSourceConnectionDto.getDatabase_type());
                String qualifiedName = metadataInstancesService.getQualifiedNameByNodeId(node, user, dataSourceConnectionDto, dataSourceDefinitionDto, taskDto.getId().toHexString());
                qualifiedNames.add(qualifiedName);
            } else if (node instanceof DatabaseNode) {
                String connectionId = ((DatabaseNode) node).getConnectionId();
                DataSourceConnectionDto dataSourceConnectionDto = dataSourceMap.get(connectionId);
                DataSourceDefinitionDto dataSourceDefinitionDto = definitionDtoMap.get(dataSourceConnectionDto.getDatabase_type());
                List<String> metas = metadataInstancesService.findDatabaseNodeQualifiedName(node.getId(), user, taskDto, dataSourceConnectionDto, dataSourceDefinitionDto);
                qualifiedNames.addAll(metas);
            }
        }

        if (CollectionUtils.isNotEmpty(qualifiedNames)) {
            //优先获取逻辑表，没有找到的话，取物理表的。
            metadataList = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedNames, user);
            Map<String, MetadataInstancesDto> qualifiedMap = metadataList.stream().collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, m -> m, (m1, m2) -> m1));
            qualifiedNames.removeAll(qualifiedMap.keySet());
            qualifiedNames = qualifiedNames.stream().map(q->{
                int i = q.lastIndexOf("_");
                return q.substring(0, i);
            }).collect(Collectors.toList());
            List<MetadataInstancesDto> metadataList1 = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedNames, user);
            for (MetadataInstancesDto metadataInstancesDto : metadataList1) {
                metadataInstancesDto.setQualifiedName(metadataInstancesDto.getQualifiedName() + "_" + taskDto.getId().toHexString());
            }
            metadataList.addAll(metadataList1);
        }


        List<MetadataInstancesDto> databaseSchemes = metadataInstancesService.findDatabaseScheme(connectionIds, user);
        metadataList.addAll(databaseSchemes);

        Criteria criteria = Criteria.where("dataFlowId").is(taskDto.getId().toHexString());
        List<MetadataTransformerDto> metadataTransformerDtos = metadataTransformerService.findAllDto(new Query(criteria), user);
        Map<String, MetadataTransformerDto> metadataTransformerDtoMap = metadataTransformerDtos.stream().collect(Collectors.toMap(m -> m.getDataFlowId() + m.getStageId(), m -> m, (m1, m2) -> m1));


        TransformerWsMessageDto transformerWsMessageDto = new TransformerWsMessageDto(taskDto, options, metadataList, dataSourceMap, definitionDtoMap, user.getUserId()
                , user.getUsername(), metadataTransformerDtoMap, MessageType.TRANSFORMER.getType());
        return transformerWsMessageDto;

    }

    public Map<String, List<Message>> transformSchema(TaskDto taskDto, UserDetail user) {
        log.debug("start transform schema, task = {}, user = {}", taskDto, user);
        TransformerWsMessageDto transformParam = getTransformParam(taskDto, user);

        taskService.updateById(taskDto.getId(), Update.update("transformUuid", transformParam.getOptions().getUuid()).set("transformed", false), user);

        sendTransformer(transformParam, user);
        return new HashMap<>();

//        DAGDataServiceImpl dagDataService1 = new DAGDataServiceImpl(transformParam);
//
//        Map<String, List<Message>> transformSchema = taskDto.getDag().transformSchema(null, dagDataService1, transformParam.getOptions());
//        TransformerWsMessageResult transformerWsMessageResult = new TransformerWsMessageResult();
//        transformerWsMessageResult.setTransformSchema(transformSchema);
//        transformerWsMessageResult.setUpsertTransformer(dagDataService1.getUpsertTransformer());
//        transformerWsMessageResult.setBatchInsertMetaDataList(dagDataService1.getBatchInsertMetaDataList());
//        transformerWsMessageResult.setUpsertItems(dagDataService1.getUpsertItems());
//        transformerWsMessageResult.setBatchMetadataUpdateMap(dagDataService1.getBatchMetadataUpdateMap());
//        transformerResult(user, transformerWsMessageResult);
//        return transformSchema;
    }

    public void transformerResult(UserDetail user, TransformerWsMessageResult result) {
        transformerResult(user, result, false);
    }
    public void transformerResult(UserDetail user, TransformerWsMessageResult result, boolean saveHistory) {

        Map<String, List<Message>> msgMap = result.getTransformSchema();
        if (MapUtils.isNotEmpty(msgMap)) {
            log.warn("transformerResult msgMap:" + JSON.toJSONString(msgMap));
            // add transformer task log
            List<String> taskIds = Lists.newArrayList();
            taskIds.addAll(msgMap.keySet());
            taskDagCheckLogService.createLog(taskIds.get(0), user.getUserId(), Level.ERROR.getValue(), DagOutputTemplateEnum.MODEL_PROCESS_CHECK,
                    false, true, DateUtil.now(), msgMap.get(taskIds.get(0)).get(0).getMsg());
        }

        metadataInstancesService.bulkSave(result.getBatchInsertMetaDataList(), result.getBatchMetadataUpdateMap(), user, saveHistory, result.getTaskId());

        if (CollectionUtils.isNotEmpty(result.getBatchRemoveMetaDataList())) {
            Criteria criteria = Criteria.where("qualified_name").in(result.getBatchRemoveMetaDataList());
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

        if (StringUtils.isNotBlank(result.getTransformUuid()) && StringUtils.isNotBlank(result.getTaskId())) {
            Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(result.getTaskId()))
                    .and("transformUuid").is(result.getTransformUuid());
            UpdateResult transformed = taskService.update(new Query(criteria), Update.update("transformed", true), user);
            if (transformed.getModifiedCount() > 0 && CollectionUtils.isNotEmpty(result.getUpsertTransformer())) {
                String taskId = result.getTaskId();
                int total = result.getUpsertTransformer().get(0).getTotal();
                // add transformer task log
                taskDagCheckLogService.createLog(taskId, user.getUserId(), Level.INFO.getValue(), DagOutputTemplateEnum.MODEL_PROCESS_CHECK,
                        false, true, DateUtil.now(), total, total);
            }
        }
    }


    private void sendTransformer(TransformerWsMessageDto data, UserDetail user) {
        TaskDto taskDto = data.getTaskDto();
        if (taskDto == null) {
            return;
        }

        Map<String, DataSourceDefinitionDto> definitionDtoMap = data.getDefinitionDtoMap();
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

        String processId = availableAgent.get(0).getProcessId();
        data.setType(MessageType.TRANSFORMER.getType());
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(processId);
        queueDto.setData(data);
        queueDto.setType("pipe");

        log.info("build send test connection websocket context, processId = {}, userId = {}, queueDto = {}", processId, user.getUserId(), queueDto);
        messageQueueService.sendMessage(queueDto);

    }
}

package com.tapdata.tm.task.service;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.lock.annotation.Lock;
import com.tapdata.tm.lock.constant.LockType;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.constant.SyncType;
import com.tapdata.tm.transform.service.MetadataTransformerItemService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.UUIDUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
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

    @Autowired
    public TransformSchemaService(DAGDataService dagDataService, MetadataInstancesService metadataInstancesService, TaskService taskService,
                                  DataSourceService dataSourceService, MetadataTransformerService metadataTransformerService,
                                  DataSourceDefinitionService definitionService, MetadataTransformerItemService metadataTransformerItemService) {
        this.dagDataService = dagDataService;
        this.metadataInstancesService = metadataInstancesService;
        this.taskService = taskService;
        this.metadataTransformerService = metadataTransformerService;
        this.dataSourceService = dataSourceService;
        this.definitionService = definitionService;
        this.metadataTransformerItemService = metadataTransformerItemService;
    }

    @Value("${tm.transform.batch.num:20}")
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

    public Map<String, List<Message>> transformSchema(TaskDto taskDto, UserDetail user) {
        log.debug("start transform schema, task = {}, user = {}", taskDto, user);
        taskDto.setUserId(user.getUserId());
        DAG dag = taskDto.getDag();
        dag.getNodes().forEach(node -> node.setService(dagDataService));

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
            public void schemaTransformResult(String nodeId, List<SchemaTransformerResult> schemaTransformerResults) {
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
            options.setUuid(UUIDUtil.getUUID());
        }
        // update metaTransformer version
        dag.getTargets().forEach(target -> metadataTransformerService.updateVersion(taskDto.getId().toHexString(), target.getId(), options.getUuid()));

        List<Node> nodes = dag.getNodes();

        if (SyncType.SYNC.getValue().equals(taskDto.getSyncType())) {
            List<MetadataInstancesDto> metadataList = new ArrayList<>();
            Map<String, DataSourceConnectionDto> dataSourceMap = new HashMap<>();
            Map<String, DataSourceDefinitionDto> definitionDtoMap = new HashMap<>();
            Map<String, TaskDto> taskMap = new HashMap<>();

            List<String> connectionIds = nodes.stream().filter(n -> n instanceof TableNode).map(n -> ((TableNode) n).getConnectionId()).collect(Collectors.toList());
            Criteria idCriteria = Criteria.where("_id").in(connectionIds);
            Query query = new Query(idCriteria);
            //TODO query 需要限制好参数
            List<DataSourceConnectionDto> dataSources = dataSourceService.findAllDto(query, user);


            if (CollectionUtils.isNotEmpty(dataSources)) {
                dataSourceMap = dataSources.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), d -> d, (d1, d2) -> d1));
                Set<String> dbTypes = dataSources.stream().map(DataSourceConnectionDto::getDatabase_type).collect(Collectors.toSet());
                List<DataSourceDefinitionDto> definitionDtos = definitionService.getByDataSourceType(new ArrayList<>(dbTypes), user);
                if (CollectionUtils.isNotEmpty(definitionDtos)) {
                    definitionDtoMap = definitionDtos.stream().collect(Collectors.toMap(DataSourceDefinitionDto::getType, d->d, (d1, d2) -> d1));
                }
            }

            List<String> qualifiedNames = new ArrayList<>();
            for (Node node : nodes) {
                if (node instanceof TableNode) {
                    String connectionId = ((TableNode) node).getConnectionId();
                    DataSourceConnectionDto dataSourceConnectionDto = dataSourceMap.get(connectionId);
                    DataSourceDefinitionDto dataSourceDefinitionDto = definitionDtoMap.get(dataSourceConnectionDto.getDatabase_type());
                    String qualifiedName = metadataInstancesService.getQualifiedNameByNodeId(node, user, dataSourceConnectionDto, dataSourceDefinitionDto);
                    qualifiedNames.add(qualifiedName);
                }
            }

            if (CollectionUtils.isNotEmpty(qualifiedNames)) {
                metadataList = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedNames, user);
            }


            List<MetadataInstancesDto> databaseSchemes = metadataInstancesService.findDatabaseScheme(connectionIds, user);
            metadataList.addAll(databaseSchemes);

            Criteria criteria = Criteria.where("dataFlowId").is(taskDto.getId().toHexString());
            List<MetadataTransformerDto> metadataTransformerDtos = metadataTransformerService.findAllDto(new Query(criteria), user);
            Map<String, MetadataTransformerDto> metadataTransformerDtoMap = metadataTransformerDtos.stream().collect(Collectors.toMap(m -> m.getDataFlowId() + m.getStageId(), m -> m, (m1, m2) -> m1));


            DAGDataServiceImpl dagDataService1 = new DAGDataServiceImpl(metadataList, dataSourceMap, definitionDtoMap, user.getUserId(), user.getUsername(), null, metadataTransformerDtoMap);

            Map<String, List<Message>> transformSchema = dag.transformSchema(null, dagDataService1, options);

            metadataInstancesService.bulkSave(dagDataService1.getBatchInsertMetaDataList(), dagDataService1.getBatchMetadataUpdateMap(), user);

            List<MetadataTransformerItemDto> upsertItems = dagDataService1.getUpsertItems();
            if (upsertItems.size() > 0) {
                metadataTransformerItemService.bulkUpsert(dagDataService1.getUpsertItems());
                MetadataTransformerItemDto itemDto = upsertItems.get(0);
                String dataFlowId = itemDto.getDataFlowId();
                String uuid = itemDto.getUuid();
                Query query1 = new Query(Criteria.where("dataFlowId").is(dataFlowId).and("uuid").ne(uuid));
                metadataTransformerItemService.deleteAll(query1);
            }

            List<MetadataTransformerDto> upsertTransformer = dagDataService1.getUpsertTransformer();
            if (upsertTransformer.size() > 0) {
                metadataTransformerService.save(upsertTransformer, user);
            }
            return transformSchema;
        }

        return dag.transformSchema(null, dagDataService, options);
    }
}

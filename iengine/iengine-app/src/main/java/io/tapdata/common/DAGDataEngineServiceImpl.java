package io.tapdata.common;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.schema.SchemaApplyResult;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.utils.GZIPUtil;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.vo.MigrateJsResultVo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.FilterMetadataInstanceUtil;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastSchemaTargetNode;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.websocket.handler.DeduceSchemaHandler;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.mongodb.core.query.Query;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;


public class DAGDataEngineServiceImpl extends DAGDataServiceImpl {
    public static final String TAG = DAGDataEngineServiceImpl.class.getSimpleName();
    private final ObsLogger obsLogger;
    private final TaskService<TaskDto> taskService;
    private final TaskDto taskDto;

    private final boolean engineDeduction;

    private final ClientMongoOperator clientMongoOperator;

    private final String uuid ;

    private final Map<String,TapTableMap<String, TapTable>> tapTableMapHashMap;
    private Map<String, List<String>> tableNodeSameMetadataInstances = new HashMap<>();

    public DAGDataEngineServiceImpl(TransformerWsMessageDto transformerWsMessageDto, TaskService<TaskDto> taskService,
                                    Map<String,TapTableMap<String, TapTable>> tapTableMapHashMap,ClientMongoOperator clientMongoOperator) {
        super(transformerWsMessageDto);
        this.obsLogger = ObsLoggerFactory.getInstance().getObsLogger(transformerWsMessageDto.getTaskDto());
        this.taskService = taskService;
        this.taskDto = transformerWsMessageDto.getTaskDto();
        this.engineDeduction = true;
        this.clientMongoOperator = clientMongoOperator;
        this.uuid = transformerWsMessageDto.getOptions().getUuid();
        this.tapTableMapHashMap = tapTableMapHashMap;
        this.tableNodeSameMetadataInstances = transformerWsMessageDto.getTableNodeSameMetadataInstances();
    }

    public DAGDataEngineServiceImpl(DeduceSchemaHandler.DeduceSchemaRequest request,TaskService<TaskDto> taskService, ClientMongoOperator clientMongoOperator) {
        super(request.getMetadataInstancesDtoList(), request.getDataSourceMap(), request.getDefinitionDtoMap(),request.getUserId(), request.getUserName(), request.getTaskDto(), request.getTransformerDtoMap());
        this.obsLogger = ObsLoggerFactory.getInstance().getObsLogger(request.getTaskDto());
        this.taskService = taskService;
        this.tapTableMapHashMap = new HashMap<>();
        this.clientMongoOperator = clientMongoOperator;
        this.taskDto = request.getTaskDto();
        this.engineDeduction = false;
        this.uuid = request.getOptions().getUuid();
    }

    @Override
    public TapTable loadTapTable(String nodeId, String virtualId, TaskDto taskDto) {
        TaskClient<TaskDto> taskClient = null;
        try {
            // 跑任务加载js模型
            String schemaKey = taskDto.getId() + "-" + virtualId;
            long startTs = System.currentTimeMillis();
            taskClient = execTask(taskDto);

            obsLogger.trace("load tapTable task {} {}, cost {}ms", schemaKey, null == taskClient ? "None" : taskClient.getStatus(), (System.currentTimeMillis() - startTs));
            //成功
            TapTable tapTable = HazelcastSchemaTargetNode.getTapTable(schemaKey);
            if (obsLogger.isDebugEnabled()) {
                obsLogger.debug("derivation results: {}", JSON.toJSONString(tapTable));
            }
            return tapTable;
        } catch (Exception e) {
            obsLogger.error("An error occurred while obtaining the results of model deduction", e);
        } finally {
            TaskClient<TaskDto> finalTaskClient = taskClient;
            CommonUtils.ignoreAnyError(() -> Optional.ofNullable(finalTaskClient).ifPresent(TaskClient::close), TAG + "-closeTaskClient");
        }
        return null;
    }

    @Override
    public List<MigrateJsResultVo> getJsResult(String jsNodeId, String virtualTargetId, TaskDto taskDto) {
        TaskClient<TaskDto> taskClient = null;
        try {
            String schemaKey = taskDto.getId() + "-" + virtualTargetId;
            long startTs = System.currentTimeMillis();

            taskClient = execTask(taskDto);

            obsLogger.trace("load MigrateJsResultVos task {} {}, cost {}ms", schemaKey, taskClient.getStatus(), (System.currentTimeMillis() - startTs));
            //成功
            List<SchemaApplyResult> schemaApplyResultList = HazelcastSchemaTargetNode.getSchemaApplyResultList(schemaKey);
            if (obsLogger.isDebugEnabled()) {
                obsLogger.debug("derivation results: {}", JSON.toJSONString(schemaApplyResultList));
            }

            if (CollectionUtils.isNotEmpty(schemaApplyResultList)) {
                return schemaApplyResultList.stream().map(s -> new MigrateJsResultVo(s.getOp(), s.getFieldName(), s.getTapField(), s.getTapIndex()))
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            obsLogger.error("An error occurred while obtaining the results of model deduction", e);
        } finally {
            TaskClient<TaskDto> finalTaskClient = taskClient;
            CommonUtils.ignoreAnyError(() -> Optional.ofNullable(finalTaskClient).ifPresent(TaskClient::close), TAG + "-closeTaskClient");
        }
        return new ArrayList<>();
    }

    protected TaskClient<TaskDto> execTask(TaskDto taskDto) {
        taskDto.setType(ParentTaskDto.TYPE_INITIAL_SYNC);
        TaskClient<TaskDto> taskClient = taskService.startTestTask(taskDto);
        Optional.ofNullable(taskClient).ifPresent(TaskClient::join);
        return taskClient;
    }
    @Override
    public void initializeModel(Boolean isSync) {
        Map<String, List<MetadataInstancesDto>> updateMetadataInstancesDtos = getBatchMetadataUpdateMap().values().stream()
                .filter(metadataInstancesDto -> SourceTypeEnum.VIRTUAL.name().equals(metadataInstancesDto.getSourceType()))
                .collect(Collectors.groupingBy(MetadataInstancesDto::getNodeId));

        Map<String, List<MetadataInstancesDto>> insertMetadataInstancesDtos = getBatchInsertMetaDataList().stream()
                .filter(metadataInstancesDto -> SourceTypeEnum.VIRTUAL.name().equals(metadataInstancesDto.getSourceType()))
                .collect(Collectors.groupingBy(MetadataInstancesDto::getNodeId));

        insertMetadataInstancesDtos.forEach((nodeId, metadataInstancesDtos) -> updateMetadataInstancesDtos.merge(nodeId,metadataInstancesDtos,(v1, v2)->{
            List<MetadataInstancesDto> metadataInstancesDtoList = new ArrayList<>(v1);
            metadataInstancesDtoList.addAll(v2);
            return metadataInstancesDtoList;
        }));

        for (Map.Entry<String, List<MetadataInstancesDto>> entry : updateMetadataInstancesDtos.entrySet()) {
            Map<String, String> tableNameQualifiedNameMap = new HashMap<>();
            List<MetadataInstancesDto> filteredList = entry.getValue();
            TapTableMap<String, TapTable> tapTableMap = tapTableMapHashMap.computeIfAbsent(entry.getKey(), key -> TapTableMap.create(null, entry.getKey(), tableNameQualifiedNameMap,taskDto.getTmCurrentTime()));
            filteredList.forEach(metadataInstancesDto -> {
                TapTable tapTable = convertTapTable(metadataInstancesDto);
                if (tapTable != null) {
                    if(isSync && metadataInstancesDto.getMetaType().equals("processor_node")){
                        tapTableMap.putNew(metadataInstancesDto.getNodeId(), tapTable, metadataInstancesDto.getQualifiedName());
                    }else{
                        tapTableMap.putNew(metadataInstancesDto.getOriginalName(), tapTable, metadataInstancesDto.getQualifiedName());
                    }
                    checkTableNodeSameMetadataInstancesDto(metadataInstancesDto,tapTable);
                }
            });
        }
        if (taskDto.getDag() != null) {
            List<Node> nodes = taskDto.getDag().getNodes();
            if (CollectionUtils.isNotEmpty(nodes)) {
                nodes.forEach(f -> {
                    if(f instanceof MergeTableNode){
                        List<Node> predecessors = taskDto.getDag().predecessors(f.getId());
                        TapTableMap<String, TapTable> tapTableMap = tapTableMapHashMap.get(f.getId());
                        if(tapTableMap == null)return;
                        predecessors.forEach(node -> {
                            TapTableMap<String, TapTable> preTapTableMap = tapTableMapHashMap.get(node.getId());
                            mergeTableMap(tapTableMap,preTapTableMap);
                        });
                    }else if(f instanceof LogCollectorNode){
                        LogCollectorNode logCollectorNode = (LogCollectorNode) f;
                        Map<String, LogCollecotrConnConfig> connConfigs = logCollectorNode.getLogCollectorConnConfigs();
                        TapTableMap<String, TapTable> tapTableMap = tapTableMapHashMap.computeIfAbsent(f.getId(), key -> TapTableMap.create(null, f.getId(), new HashMap<>(),taskDto.getTmCurrentTime()));
                        Map<String,MetadataInstancesDto> metadataInstancesDtoHashMap = getLogCollectorMetadataInstancesDto().stream()
                                .collect(Collectors.toMap(v -> v.getDatabaseId() + "." + v.getOriginalName()
                                        , metadataInstancesDto -> metadataInstancesDto, (m1, m2) -> m1));
                        if (null != connConfigs && !connConfigs.isEmpty()) {
                            Map<String,List<String>> connections = ShareCdcUtil.getConnectionIds(f, ids -> {
                                Query connectionQuery = new Query(where("_id").in(ids));
                                connectionQuery.fields().include("_id").include("namespace");
                                return clientMongoOperator.find(connectionQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
                            }).stream().collect(Collectors.toMap(Connections::getId,Connections::getNamespace,(value1, value2) -> value1));
                            metadataInstancesDtoHashMap.values().forEach(metadataInstancesDto -> {
                                if(null != connections.get(metadataInstancesDto.getSource().get_id())){
                                    String namespace =String.join(".",connections.get(metadataInstancesDto.getSource().get_id()));
                                    String originalName = String.join(".", namespace, metadataInstancesDto.getOriginalName());
                                    tapTableMap.putNew(originalName,convertTapTable(metadataInstancesDto),metadataInstancesDto.getQualifiedName());
                                }
                            });
                        }else{
                            for(MetadataInstancesDto metadataInstancesDto :metadataInstancesDtoHashMap.values()){
                                tapTableMap.putNew(metadataInstancesDto.getOriginalName(),convertTapTable(metadataInstancesDto),metadataInstancesDto.getQualifiedName());
                            }
                        }
                    }
                    f.setSchema(null);
                    f.setOutputSchema(null);
                });
            }
        }
        clearTransformer();
    }

    @Override
    public Boolean whetherEngineDeduction() {
        return this.engineDeduction;
    }


    protected TapTable convertTapTable(MetadataInstancesDto item){
        FilterMetadataInstanceUtil.filterMetadataInstancesFields(item);
        TapTable tapTable = PdkSchemaConvert.toPdk(item);
        TapTableUtil.sortFieldMap(tapTable);
        return tapTable;
    }

    @Override
    public void uploadModel(Map<String, List<Message>> transformSchema){
        TransformerWsMessageResult wsMessageResult = new TransformerWsMessageResult();
        wsMessageResult.setBatchMetadataUpdateMap(getBatchMetadataUpdateMap());
        wsMessageResult.setBatchInsertMetaDataList(getBatchInsertMetaDataList());
        wsMessageResult.setUpsertItems(getUpsertItems());
        wsMessageResult.setUpsertTransformer(getUpsertTransformer());
        wsMessageResult.setTransformSchema(transformSchema);
        wsMessageResult.setTaskId(taskDto.getId().toHexString());
        wsMessageResult.setTransformUuid(uuid);
        if (taskDto.getDag() != null) {
            List<Node> nodes = taskDto.getDag().getNodes();
            if (CollectionUtils.isNotEmpty(nodes)) {
                nodes.forEach(f -> {
                    f.setSchema(null);
                    f.setOutputSchema(null);
                });
            }
        }
        wsMessageResult.setDag(taskDto.getDag());
        String jsonResult = JsonUtil.toJsonUseJackson(wsMessageResult);
        byte[] gzip = GZIPUtil.gzip(jsonResult.getBytes());
        byte[] encode = Base64.getEncoder().encode(gzip);
        String dataString = new String(encode, StandardCharsets.UTF_8);
        clientMongoOperator.insertOne(dataString, ConnectorConstant.TASK_COLLECTION + "/transformer/resultV2");
    }

    protected void mergeTableMap(TapTableMap<String, TapTable> tapTableMap , TapTableMap<String, TapTable> preTapTableMap) {
        for(String key : preTapTableMap.keySet()){
            tapTableMap.putNew(key,preTapTableMap.get(key),preTapTableMap.getQualifiedName(key));
        }
    }

    protected void checkTableNodeSameMetadataInstancesDto(MetadataInstancesDto metadataInstancesDto,TapTable tapTable){
         if(null != tableNodeSameMetadataInstances && tableNodeSameMetadataInstances.containsKey(metadataInstancesDto.getQualifiedName())){
             tableNodeSameMetadataInstances.get(metadataInstancesDto.getQualifiedName()).forEach(n -> {
                 if(!metadataInstancesDto.getNodeId().equals(n)){
                     TapTableMap<String, TapTable> tapTableMap = tapTableMapHashMap.computeIfAbsent(n, key -> TapTableMap.create(null, n, new HashMap<>(),taskDto.getTmCurrentTime()));
                     tapTableMap.putNew(metadataInstancesDto.getOriginalName(), tapTable, metadataInstancesDto.getQualifiedName());
                 }
         });
         }
    }

}

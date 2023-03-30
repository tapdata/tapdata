package com.tapdata.tm.metadatainstance.service.impl;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dag.service.DAGService;
import com.tapdata.tm.metadatainstance.dto.MigrateResetTableDto;
import com.tapdata.tm.metadatainstance.dto.MigrateTableInfoDto;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.service.MetaMigrateService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MetaMigrateServiceImpl implements MetaMigrateService {

    private DAGService dagService;
    private TaskService taskService;
    private MetadataInstancesService metadataInstancesService;
    private MongoTemplate mongoTemplate;

    public MetaMigrateServiceImpl(MetadataInstancesService metadataInstancesService) {
        this.metadataInstancesService = metadataInstancesService;
    }

    @Override
    public void saveMigrateTableInfo(MigrateTableInfoDto tableInfo, UserDetail userDetail) {
        String taskId = tableInfo.getTaskId();
        String tableName = tableInfo.getTableName();
        String nodeId = tableInfo.getNodeId();

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));

        DAG dag = taskDto.getDag();

        LinkedList<DatabaseNode> databaseNodes = dag.getNodes().stream().filter(node -> node instanceof DatabaseNode)
                .map(node -> (DatabaseNode) node)
                .collect(Collectors.toCollection(LinkedList::new));
        if (CollectionUtils.isEmpty(databaseNodes)) {
            return;
        }
        DatabaseNode sourceNode = dag.getSourceNode(nodeId);
        DatabaseNode targetNode = dag.getTargetNode(nodeId);

        Map<String, MigrateTableInfoDto.Field> fieldMap = tableInfo.getFields().stream()
                .collect(Collectors.toMap(MigrateTableInfoDto.Field::getFieldName, Function.identity()));

        List<MetadataInstancesDto> instancesDtos = metadataInstancesService.findByNodeId(nodeId, userDetail);
        Optional<MetadataInstancesDto> first = instancesDtos.stream().filter(meta -> tableName.equals(meta.getName())).findFirst();
        first.ifPresent(schema -> {
            schema.getFields().forEach(f -> {
                if (fieldMap.containsKey(f.getOriginalFieldName())) {
                    MigrateTableInfoDto.Field field = fieldMap.get(f.getOriginalFieldName());
//                    f.setDefaultValue(field.getDefaultValue());
                    // Modification type is not supported at present
//                    f.setDataType(field.getFieldType());
                    f.setUseDefaultValue(field.isUseDefaultValue());
                    FunctionUtils.isTureOrFalse(field.isUseDefaultValue()).trueOrFalseHandle(
                            () -> {
                                f.setDefaultValue(Objects.isNull(field.getDefaultValue()) ? f.getOriginalDefaultValue() : field.getDefaultValue());
                                f.setSource("manual");
                            },
                            () -> f.setDefaultValue(null));
                }
            });
            metadataInstancesService.save(schema, userDetail);
        });
    }

    @Override
    public void migrateResetAllTable(MigrateResetTableDto dto, UserDetail userDetail) {
        String taskId = dto.getTaskId();
        String nodeId = dto.getNodeId();

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));

        DAG dag = taskDto.getDag();
        DatabaseNode sourceNode = dag.getSourceNode(nodeId);
        DatabaseNode targetNode = dag.getTargetNode(nodeId);

        List<String> tableNames = sourceNode.getTableNames();
        if (CollectionUtils.isEmpty(tableNames)) {
            return;
        }

        List<MetadataInstancesEntity> tableNameList = null;


        if (targetNode != null) {
            tableNameList = metadataInstancesService.findEntityBySourceIdAndTableNameList(
                    targetNode.getConnectionId(), tableNames, userDetail, taskId);
        }

        if (CollectionUtils.isNotEmpty(tableNameList)) {
            List<Pair<Query, Update>> updateList = new ArrayList<>();
            tableNameList.forEach(table -> {
                AtomicBoolean needUpdate = new AtomicBoolean(false);
                table.getFields().forEach(field -> {
                    if (!Objects.equals(field.getOriginalDefaultValue(), field.getDefaultValue())) {
                        field.setDefaultValue(field.getOriginalDefaultValue());
                        needUpdate.set(true);
                    }
                });
                if (needUpdate.get()) {
                    Criteria criteria = Criteria.where("_id").is(table.getId());
                    Update update = metadataInstancesService.buildUpdateSet(table);
                    updateList.add(Pair.of(Query.query(criteria), update));
                }
            });
            if (CollectionUtils.isNotEmpty(updateList)) {
                BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, MetadataInstancesEntity.class);
                bulkOperations.upsert(updateList);
                bulkOperations.execute();
            }
        }
    }
}



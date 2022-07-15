package com.tapdata.tm.metadatainstance.service.impl;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dag.service.DAGService;
import com.tapdata.tm.metadatainstance.dto.MigrateResetTableDto;
import com.tapdata.tm.metadatainstance.dto.MigrateTableInfoDto;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.service.MetaMigrateService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));

        DAG dag = taskDto.getDag();

        LinkedList<DatabaseNode> databaseNodes = dag.getNodes().stream().filter(node -> node instanceof DatabaseNode)
                .map(node -> (DatabaseNode) node)
                .collect(Collectors.toCollection(LinkedList::new));
        if (CollectionUtils.isEmpty(databaseNodes)) {
            return;
        }
        DatabaseNode sourceNode = databaseNodes.getFirst();
        DatabaseNode targetNode = databaseNodes.getLast();

        Map<String, MigrateTableInfoDto.Field> fieldMap = tableInfo.getFields().stream()
                .collect(Collectors.toMap(MigrateTableInfoDto.Field::getFieldName, Function.identity()));

        MetadataInstancesDto metadataInstancesDto = metadataInstancesService.findBySourceIdAndTableName(targetNode.getConnectionId(), tableName, userDetail);
        if (Objects.nonNull(metadataInstancesDto)) {
            metadataInstancesDto.getFields().forEach(f -> {
                if (fieldMap.containsKey(f.getFieldName())) {
                    MigrateTableInfoDto.Field field = fieldMap.get(f.getFieldName());
                    f.setDataType(field.getFieldType());
                    f.setDefaultValue(field.getDefaultValue());
                }
            });
            metadataInstancesService.save(metadataInstancesDto, userDetail);
        } else {
            Schema schema = dagService.loadSchema(userDetail.getUserId(), MongoUtils.toObjectId(sourceNode.getConnectionId()), tableName);

            schema.getFields().forEach(f -> {
                if (fieldMap.containsKey(f.getFieldName())) {
                    MigrateTableInfoDto.Field field = fieldMap.get(f.getFieldName());
                    f.setDataType(field.getFieldType());
                    f.setDefaultValue(field.getDefaultValue());
                }
            });

            dagService.createOrUpdateSchema(userDetail.getUserId(), MongoUtils.toObjectId(targetNode.getConnectionId()),
                    Lists.newArrayList(schema), null, targetNode);
        }
    }

    @Override
    public void migrateResetAllTable(MigrateResetTableDto dto, UserDetail userDetail) {
        String taskId = dto.getTaskId();

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));

        DAG dag = taskDto.getDag();
        DatabaseNode sourceNode = dag.getSourceNode().getFirst();
        DatabaseNode targetNode = dag.getTargetNode().getLast();

        List<String> tableNames = sourceNode.getTableNames();
        if (CollectionUtils.isEmpty(tableNames) && !StringUtils.equals("all", sourceNode.getMigrateTableSelectType())) {
            return;
        }

        List<MetadataInstancesEntity> tableNameList = metadataInstancesService.findEntityBySourceIdAndTableNameList(targetNode.getConnectionId(), tableNames, userDetail);

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



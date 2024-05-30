package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.entity.conversion.PossibleDataTypes;
import io.tapdata.pdk.apis.entity.Capability;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component("targetSettingStrategy")
@Setter(onMethod_ = {@Autowired})
public class TargetSettingStrategyImpl implements DagLogStrategy {
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.TARGET_NODE_CHECK;

    private final String FINAL_SYNC_INDEX="syncIndex";

    private final String FINAL_STRING_DROPTABLE = "dropTable";

    private MetadataInstancesService metadataInstancesService;
    private TaskDagCheckLogService taskDagCheckLogService;

    private DataSourceService dataSourceService;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
        String taskId = taskDto.getId().toHexString();
        List<TaskDagCheckLog> result = Lists.newArrayList();
        DAG dag = taskDto.getDag();

        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getTargets())) {
            return null;
        }

        String userId = userDetail.getUserId();
        dag.getTargets().forEach(node -> {
            String name = node.getName();
            String nodeId = node.getId();

            DataParentNode dataParentNode = (DataParentNode) node;
            String connectionId = dataParentNode.getConnectionId();

            if (StringUtils.isEmpty(name)) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_NAME_EMPTY"), dataParentNode.getDatabaseType());
                result.add(log);
            }

            if (StringUtils.isEmpty(connectionId)) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_NAME_NOT_SELECT_DB"), name);
                result.add(log);
            }

            AtomicReference<List<String>> tableNames = new AtomicReference<>();
            if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                DatabaseNode databaseNode = (DatabaseNode) node;
                Optional.ofNullable(databaseNode.getSyncObjects()).ifPresent(list -> tableNames.set(list.get(0).getObjectNames()));
            } else {
                TableNode tableNode = (TableNode) node;
                tableNames.set(Lists.newArrayList(tableNode.getTableName()));
                if ("updateOrInsert".equals(tableNode.getWriteStrategy())) {
                    List<String> updateConditionFields = tableNode.getUpdateConditionFields();
                    if (CollectionUtils.isEmpty(updateConditionFields)) {
                        TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_NAME_UPDATE_ERROR"), name);
                        result.add(log);
                    }/* else {
                    //根据#139501 缺陷转需求，去掉模型中是否包含更新字段的校验。
                        List<MetadataInstancesDto> nodeSchemas = metadataInstancesService.findByNodeId(nodeId, userDetail);
                        Optional.ofNullable(nodeSchemas).flatMap(list -> list.stream().filter(i -> tableNode.getTableName().equals(i.getName())).findFirst()).ifPresent(schema -> {
                            List<String> fields = schema.getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
                            List<String> noExistsFields = updateConditionFields.stream().filter(d -> !fields.contains(d)).collect(Collectors.toList());
                            if (CollectionUtils.isNotEmpty(noExistsFields)) {
                                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_NAME_UPDATE_NOT_EXISTS"), name, JSON.toJSON(noExistsFields));
                                result.add(log);
                            }
                        });
                    }*/
                }
            }
            checkNodeExistDataMode(locale, taskId, result, userId, node, name);
            checkNodeSyncIndex(locale, taskId, result, userId, node, name);
            checkTargetUpdateField(locale, taskId ,result , userId ,node , name ,connectionId);
            if (CollectionUtils.isEmpty(tableNames.get())) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_NOT_SELECT_TB"), name);
                result.add(log);
            }

            String databaseType = dataParentNode.getDatabaseType();
            List<MetadataInstancesDto> schemaList = metadataInstancesService.findByNodeId(nodeId, userDetail);
            if (CollectionUtils.isNotEmpty(schemaList)) {
                for (MetadataInstancesDto metadata : schemaList) {
                    if (Lists.newArrayList("Oracle", "Clickhouse").contains(databaseType)) {
                        for (Field field : metadata.getFields()) {
                            switch (databaseType) {
                                case "Oracle":
                                    if (Objects.nonNull(field.getIsNullable()) && !(Boolean) field.getIsNullable()) {
                                        TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_ORACLE_FIELD_EMPTY_TIP"), metadata.getName(), field.getFieldName());
                                        result.add(log);
                                    }
                                    break;
                                case "Clickhouse":
                                    if (Objects.nonNull(field.getPrimaryKey()) && field.getPrimaryKey() &&
                                            (field.getDataType().contains("Float32") ||
                                                    field.getDataType().contains("Float64") ||
                                                    field.getDataType().contains("Decimal"))) {
                                        TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_CK_FIELD_FLOAT_TIP"), metadata.getName());
                                        result.add(log);
                                    }
                                    break;
                            }
                        }
                    }
                    // check source schema field not support
                    Map<String, PossibleDataTypes> findPossibleDataTypes = metadata.getFindPossibleDataTypes();
                    Map<String, Field> fieldMap = metadata.getFields().stream().collect(Collectors.toMap(Field::getFieldName, Function.identity()));
                    if (Objects.nonNull(findPossibleDataTypes)) {
                        findPossibleDataTypes.forEach((k, v) -> {
                            Field field = fieldMap.get(k);
                            if (CollectionUtils.isEmpty(v.getDataTypes()) && field.getDataTypeTemp()!= null && Objects.equals(field.getDataType(), field.getDataTypeTemp())) {
                                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_SETTING_CHECK_FIELD"), node.getName(), metadata.getName(), k);
                                result.add(log);
                            }
                        });
                    }
                }
            }
        });

        return result;
    }

    protected void checkTargetUpdateField(Locale locale, String taskId, List<TaskDagCheckLog> result, String userId, Node node, String name, String connectionId) {
        DataSourceConnectionDto connectionDto = dataSourceService.findByIdByCheck(MongoUtils.toObjectId(connectionId));
        Optional.ofNullable(connectionDto).ifPresent(dto -> {
            if (CollectionUtils.isNotEmpty(dto.getCapabilities())) {
                boolean canCreateIndex = dto.getCapabilities().stream().map(Capability::getId).anyMatch("create_index_function"::equals);
                if(canCreateIndex){
                    TaskDagCheckLog updateFieldLog = taskDagCheckLogService.createLog(taskId, node.getId(), userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_SETTING_WRAN_UPDATEFIELD"), name);
                    result.add(updateFieldLog);
                }
            }
        });
    }

    protected void checkNodeSyncIndex(Locale locale, String taskId, List<TaskDagCheckLog> result, String userId, Node node, String name) {
        String nodeId = node.getId();
        if(node instanceof DatabaseNode){
            Map<String, Object> nodeConfig = ((DatabaseNode) node).getNodeConfig();
            if (nodeConfig != null && Boolean.TRUE.equals(nodeConfig.get(FINAL_SYNC_INDEX))) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_SETTING_CHECK_SYNCINDEX"), name);
                result.add(log);
            }
        } else if (node instanceof TableNode) {
            Map<String, Object> nodeConfig = ((TableNode) node).getNodeConfig();
            if (nodeConfig != null && Boolean.TRUE.equals(nodeConfig.get(FINAL_SYNC_INDEX))) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_SETTING_CHECK_SYNCINDEX"), name);
                result.add(log);
            }
        }
    }

    protected void checkNodeExistDataMode(Locale locale, String taskId, List<TaskDagCheckLog> result, String userId, Node node, String name) {
        String nodeId = node.getId();
        if (node instanceof DatabaseNode) {
            if (FINAL_STRING_DROPTABLE.equals(((DatabaseNode) node).getExistDataProcessMode())) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_SETTING_CHECK_EXISTDATAMODE"), name);
                result.add(log);
            }
        } else if (node instanceof TableNode) {
            if (FINAL_STRING_DROPTABLE.equals(((TableNode) node).getExistDataProcessMode())) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_SETTING_CHECK_EXISTDATAMODE"), name);
                result.add(log);
            }
        }
    }
}

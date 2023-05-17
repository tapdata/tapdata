package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import io.tapdata.entity.conversion.PossibleDataTypes;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Component("targetSettingStrategy")
@Setter(onMethod_ = {@Autowired})
public class TargetSettingStrategyImpl implements DagLogStrategy {
    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.TARGET_NODE_CHECK;

    private MetadataInstancesService metadataInstancesService;
    private TaskDagCheckLogService taskDagCheckLogService;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
        String taskId = taskDto.getId().toHexString();
        String current = DateUtil.now();
        Date now = new Date();

        List<TaskDagCheckLog> result = Lists.newArrayList();
        Set<String> nameSet = Sets.newHashSet();
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

//            boolean keepTargetSchema = false;
            AtomicReference<List<String>> tableNames = new AtomicReference<>();
//            List<String> existDataModeList = Lists.newArrayList("keepData", "removeData");
            if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                DatabaseNode databaseNode = (DatabaseNode) node;
                Optional.ofNullable(databaseNode.getSyncObjects()).ifPresent(list -> tableNames.set(list.get(0).getObjectNames()));

//                if (existDataModeList.contains(databaseNode.getExistDataProcessMode())) {
//                    keepTargetSchema = true;
//                }
            } else {
                TableNode tableNode = (TableNode) node;
//                if (existDataModeList.contains(tableNode.getExistDataProcessMode())) {
//                    keepTargetSchema = true;
//                }

                tableNames.set(Lists.newArrayList(tableNode.getTableName()));
                if ("updateOrInsert".equals(tableNode.getWriteStrategy())) {
                    List<String> updateConditionFields = tableNode.getUpdateConditionFields();
                    if (CollectionUtils.isEmpty(updateConditionFields)) {
                        TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_NAME_UPDATE_ERROR"), name);
                        result.add(log);
                    } else {
                        List<MetadataInstancesDto> nodeSchemas = metadataInstancesService.findByNodeId(nodeId, userDetail);
                        Optional.ofNullable(nodeSchemas).flatMap(list -> list.stream().filter(i -> tableNode.getTableName().equals(i.getName())).findFirst()).ifPresent(schema -> {
                            List<String> fields = schema.getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
                            List<String> noExistsFields = updateConditionFields.stream().filter(d -> !fields.contains(d)).collect(Collectors.toList());
                            if (CollectionUtils.isNotEmpty(noExistsFields)) {
                                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_NAME_UPDATE_NOT_EXISTS"), name, JSON.toJSON(noExistsFields));
                                result.add(log);
                            }
                        });
                    }
                }
            }

//            if (keepTargetSchema) {
//                List<MetadataInstancesDto> schemaList = metadataInstancesService.findSourceSchemaBySourceId(connectionId, tableNames.get(), userDetail);
//                List<String> collect = schemaList.stream().map(MetadataInstancesDto::getName).collect(Collectors.toList());
//                if (CollectionUtils.isNotEmpty(collect)) {
//                    List<String> list = new ArrayList<>(tableNames.get());
//                    list.removeAll(collect);
//                    if (CollectionUtils.isNotEmpty(list)) {
//                        TaskDagCheckLog log = TaskDagCheckLog.builder()
//                                .taskId(taskId)
//                                .checkType(templateEnum.name())
//                                .log(MessageFormat.format(MessageUtil.getDagCheckMsg(locale, "TARGET_SETTING_CHECK_SCHAME"), node.getName(), JSON.toJSONString(list)))
//                                .grade(Level.ERROR)
//                                .nodeId(node.getId()).build();
//                        log.setCreateAt(now);
//                        log.setCreateUser(userId);
//
//                        result.add(log);
//                    }
//                } else {
//                    TaskDagCheckLog log = TaskDagCheckLog.builder()
//                            .taskId(taskId)
//                            .checkType(templateEnum.name())
//                            .log(MessageFormat.format(MessageUtil.getDagCheckMsg(locale, "TARGET_SETTING_CHECK_SCHAME"), node.getName(), JSON.toJSONString(tableNames.get())))
//                            .grade(Level.ERROR)
//                            .nodeId(node.getId()).build();
//                    log.setCreateAt(now);
//                    log.setCreateUser(userId);
//
//                    result.add(log);
//                }
//            }

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
                    if (Objects.nonNull(findPossibleDataTypes)) {
                        findPossibleDataTypes.forEach((k, v) -> {
                            if (Objects.isNull(v.getLastMatchedDataType())) {
                                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "TARGET_SETTING_CHECK_FIELD"), node.getName(), metadata.getName(), k);
                                result.add(log);
                            }
                        });
                    }
                }
            }

            String template;
            Level grade;
            if (nameSet.contains(name)) {
                template = MessageUtil.getDagCheckMsg(locale, "TARGET_NODE_ERROR");
                grade = Level.ERROR;
            } else {
                template = MessageUtil.getDagCheckMsg(locale, "TARGET_NODE_INFO");
                grade = Level.INFO;
            }
            nameSet.add(name);

            String content = MessageFormat.format(template, current, name);

            TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name()).log(content)
                    .grade(grade).nodeId(nodeId).build();

            log.setCreateAt(now);
            log.setCreateUser(userId);
            result.add(log);
        });

        return result;
    }
}

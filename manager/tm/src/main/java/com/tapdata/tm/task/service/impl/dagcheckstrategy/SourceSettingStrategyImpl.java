package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.tapdata.tm.commons.dag.DAG;
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
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.entity.conversion.PossibleDataTypes;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.pdk.apis.entity.Capability;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component("sourceSettingStrategy")
@Setter(onMethod_ = {@Autowired})
public class SourceSettingStrategyImpl implements DagLogStrategy {

    private DataSourceService dataSourceService;
    private MetadataInstancesService metadataInstancesService;
    private TaskDagCheckLogService taskDagCheckLogService;

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.SOURCE_SETTING_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
        String taskId = taskDto.getId().toHexString();
        Date now = new Date();

        List<TaskDagCheckLog> result = Lists.newArrayList();
        Set<String> nameSet = Sets.newHashSet();
        DAG dag = taskDto.getDag();

        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getNodes())) {
            return null;
        }

        String userId = userDetail.getUserId();
        dag.getSources().forEach(node -> {
            String name = node.getName();
            String nodeId = node.getId();

            DataParentNode dataParentNode = (DataParentNode) node;

            if (StringUtils.isEmpty(name)) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_NAME_EMPTY"), dataParentNode.getDatabaseType());
                result.add(log);
            }

            if (StringUtils.isEmpty(dataParentNode.getConnectionId())) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_NOT_SELECT_DB"), name);
                result.add(log);
            }

            List<String> tableNames;
            String migrateSelectType = "";
            String tableExpression = "";
            if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                DatabaseNode databaseNode = (DatabaseNode) node;
                tableNames = databaseNode.getTableNames();
                migrateSelectType = databaseNode.getMigrateTableSelectType();
                tableExpression = databaseNode.getTableExpression();
            } else {
                tableNames = Lists.newArrayList(((TableNode) node).getTableName());
            }

            if (CollectionUtils.isEmpty(tableNames)) {
                if ("expression".equals(migrateSelectType)) {
                    if (StringUtils.isEmpty(tableExpression)) {
                        TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_EXP_EMPTY"), name);
                        result.add(log);
                    }
                } else {
                    TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_NOT_SELECT_TB"), name);
                    result.add(log);
                }
            }

            if (nameSet.contains(name)) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_NAME_REPEAT"), name);
                result.add(log);
            }
            nameSet.add(name);

            // check schema
            String connectionId = dataParentNode.getConnectionId();
            DataSourceConnectionDto connectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId));
            Optional.ofNullable(connectionDto).ifPresent(dto -> {
                List<String> tables = metadataInstancesService.tables(connectionId, SourceTypeEnum.SOURCE.name());

                if (CollectionUtils.isNotEmpty(dto.getCapabilities())) {
                    List<String> capList = dto.getCapabilities().stream().map(Capability::getId)
                            .filter(id -> Lists.of("stream_read_function", "batch_read_function").contains(id)).collect(Collectors.toList());

                    if (Lists.of("Tidb", "Doris").contains(connectionDto.getDatabase_type()) &&
                            connectionDto.getConfig().containsKey("enableIncrement") &&
                            !(Boolean) connectionDto.getConfig().get("enableIncrement")) {
                        capList.remove("stream_read_function");
                    }

                    boolean streamReadNotMatch = taskDto.getType().contains("cdc") && !capList.contains("stream_read_function");
                    boolean batchReadNotMatch = taskDto.getType().contains("initial_sync") && !capList.contains("batch_read_function");

                    if (streamReadNotMatch || batchReadNotMatch) {
                        TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_CHECK_TYPE"), name, JSON.toJSONString(capList), taskDto.getType());
                        result.add(log);
                    }
                }

                if (CollectionUtils.isEmpty(tables)) {
                    TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_ERROR_SCHEMA"), name);
                    result.add(log);
                } else {
                    if (!StringUtils.equals("finished", connectionDto.getLoadFieldsStatus())) {
                        TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_ERROR_SCHEMA_LOAD"), name);
                        result.add(log);
                    }

                    List<MetadataInstancesDto> schemaList = metadataInstancesService.findSourceSchemaBySourceId(connectionId, tableNames, userDetail);
                    if (CollectionUtils.isNotEmpty(schemaList)) {
                        List<String> list = schemaList.stream().map(MetadataInstancesDto::getName).collect(Collectors.toList());
                        List<String> temp = new ArrayList<>(tableNames);
                        temp.removeAll(list);
                        if (CollectionUtils.isNotEmpty(temp)) {
                            TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_CHECK_SCHAME"), node.getName(), JSON.toJSONString(temp));
                            result.add(log);
                        }

                        // check source schema field not support
                        schemaList.forEach(sch -> {
                            String tableName = sch.getName();
                            List<Field> fields = sch.getFields();
                            if (CollectionUtils.isNotEmpty(fields)) {
                                fields.forEach(k -> {
                                    TapType tapType = JSON.parseObject(k.getTapType(), TapType.class);
                                    if (TapType.TYPE_RAW == tapType.getType()) {
                                        TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_CHECK_FIELD"), node.getName(), tableName, k);
                                        result.add(log);
                                    }
                                });
                            }
                        });
                    } else {
                        TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.ERROR, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_CHECK_SCHAME"), node.getName(), JSON.toJSONString(tableNames));
                        result.add(log);
                    }
                }

                // check mariadb
                if ("mariadb".equals(dto.getDefinitionPdkId()) && taskDto.getType().contains("cdc")) {
                    TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.WARN, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_CHECK_MARIADB"), node.getName());
                    result.add(log);
                }
            });

            if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                TaskDagCheckLog log = taskDagCheckLogService.createLog(taskId, nodeId, userId, Level.INFO, templateEnum, MessageUtil.getDagCheckMsg(locale, "SOURCE_SETTING_INFO"), node.getName());
                result.add(log);
            }
        });
        return result;
    }
}

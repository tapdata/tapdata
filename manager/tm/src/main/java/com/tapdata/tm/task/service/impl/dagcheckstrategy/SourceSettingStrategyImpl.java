package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.google.common.collect.Sets;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.task.constant.DagOutputTemplate;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;

@Component("sourceSettingStrategy")
@Setter(onMethod_ = {@Autowired})
public class SourceSettingStrategyImpl implements DagLogStrategy {

    private DataSourceService dataSourceService;
    private MetadataInstancesService metadataInstancesService;

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.SOURCE_SETTING_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
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
                TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                        .grade(Level.ERROR).nodeId(nodeId)
                        .log(MessageFormat.format("$date【$taskName】【源节点设置检测】：源节点{0}节点名称为空。", dataParentNode.getDatabaseType()))
                        .build();
                log.setCreateAt(now);
                log.setCreateUser(userId);
                result.add(log);
            }

            if (StringUtils.isEmpty(dataParentNode.getConnectionId())) {
                TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                        .grade(Level.ERROR).nodeId(nodeId)
                        .log(MessageFormat.format("$date【$taskName】【源节点设置检测】：源节点{0}未选择数据库。", name))
                        .build();
                log.setCreateAt(now);
                log.setCreateUser(userId);
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
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.ERROR).nodeId(nodeId)
                                .log(MessageFormat.format("$date【$taskName】【源节点设置检测】：源节点{0}过滤条件设置异常。", name))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }
                } else {
                    TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                            .grade(Level.ERROR).nodeId(nodeId)
                            .log(MessageFormat.format("$date【$taskName】【源节点设置检测】：源节点{0}未选择表。", name))
                            .build();
                    log.setCreateAt(now);
                    log.setCreateUser(userId);
                    result.add(log);
                }
            }

            if (nameSet.contains(name)) {
                TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                        .log(MessageFormat.format("$date【$taskName】【源节点设置检测】：节点{0}检测未通过，异常项'{节点名称}'，异常原因：节点名称重复，请重新设置", name))
                        .grade(Level.ERROR)
                        .nodeId(node.getId()).build();

                log.setCreateAt(now);
                log.setCreateUser(userId);
                result.add(log);
            }
            nameSet.add(name);

            // check schema
            String connectionId = dataParentNode.getConnectionId();
            DataSourceConnectionDto connectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId));
            Optional.ofNullable(connectionDto).ifPresent(dto -> {
                List<String> tables = metadataInstancesService.tables(connectionId, SourceTypeEnum.SOURCE.name());

                if (CollectionUtils.isEmpty(tables)) {
                    TaskDagCheckLog log = TaskDagCheckLog.builder()
                            .taskId(taskId)
                            .checkType(templateEnum.name())
                            .log(MessageFormat.format(DagOutputTemplate.SOURCE_SETTING_ERROR_SCHEMA, name))
                            .grade(Level.ERROR)
                            .nodeId(nodeId).build();
                    log.setCreateAt(now);
                    log.setCreateUser(userId);
                    result.add(log);
                } else {
                    if (!StringUtils.equals("finished", connectionDto.getLoadFieldsStatus())) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder()
                                .taskId(taskId)
                                .checkType(templateEnum.name())
                                .log(MessageFormat.format(DagOutputTemplate.SOURCE_SETTING_ERROR_SCHEMA_LOAD, name))
                                .grade(Level.ERROR)
                                .nodeId(nodeId).build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }
                }
            });

            if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                TaskDagCheckLog log = TaskDagCheckLog.builder()
                        .taskId(taskId)
                        .checkType(templateEnum.name())
                        .log(MessageFormat.format(templateEnum.getInfoTemplate(), node.getName()))
                        .grade(Level.INFO)
                        .nodeId(node.getId()).build();
                log.setCreateAt(now);
                log.setCreateUser(userId);

                result.add(log);
            }
        });
        return result;
    }
}

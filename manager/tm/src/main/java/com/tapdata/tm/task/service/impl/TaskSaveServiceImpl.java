package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskSaveServiceImpl implements TaskSaveService {
    private TaskService taskService;
    private TaskDagCheckLogService taskDagCheckLogService;
    private MetadataInstancesService metadataInstancesService;

    @Override
    public boolean taskSaveCheckLog(TaskDto taskDto, UserDetail userDetail) {
        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            return false;
        }

        taskDagCheckLogService.removeAllByTaskId(taskDto.getId().toHexString());

        boolean noPass = false;
        List<TaskDagCheckLog> taskDagCheckLogs = taskDagCheckLogService.dagCheck(taskDto, userDetail, true);
        if (CollectionUtils.isNotEmpty(taskDagCheckLogs)) {
            Optional<TaskDagCheckLog> any = taskDagCheckLogs.stream().filter(log -> StringUtils.equals(Level.ERROR.getValue(), log.getGrade())).findAny();
            if (any.isPresent()) {
                noPass = true;

                taskService.updateStatus(taskDto.getId(), TaskDto.STATUS_EDIT);
            }
        }

        return noPass;
    }

    @Override
    public void syncTaskSetting(TaskDto taskDto, UserDetail userDetail) {
        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            return;
        }

        DAG dag = taskDto.getDag();

        //supplier migrate tableSelectType=all tableNames and SyncObjects
        if (CollectionUtils.isNotEmpty(dag.getSourceNode())) {
            DatabaseNode sourceNode = dag.getSourceNode().getFirst();
            List<String> tableNames = sourceNode.getTableNames();
            if (CollectionUtils.isEmpty(tableNames) && StringUtils.equals("all", sourceNode.getMigrateTableSelectType())) {
                String connectionId = sourceNode.getConnectionId();
                List<MetadataInstancesDto> metaList = metadataInstancesService.findBySourceIdAndTableNameListNeTaskId(connectionId, null, userDetail);
                if (CollectionUtils.isNotEmpty(metaList)) {
                    List<String> collect = metaList.stream().map(MetadataInstancesDto::getOriginalName).collect(Collectors.toList());
                    sourceNode.setTableNames(collect);
                }
            }

            nodeCheckData(sourceNode.successors(), tableNames, null);

            Dag temp = new Dag(dag.getEdges(), dag.getNodes());
            DAG.build(temp);
        }

    }

    private void nodeCheckData(List<Node<List<Schema>>> nodes, List<String> tableNames, Map<String, String> renameMap) {
        if (Objects.isNull(nodes) || CollectionUtils.isEmpty(nodes)) {
            return;
        }

        Node<List<Schema>> node = nodes.get(0);
        if (node instanceof TableRenameProcessNode) {
            TableRenameProcessNode tableNode = (TableRenameProcessNode) node;

            if (CollectionUtils.isEmpty(tableNames)) {
                tableNode.setTableNames(new LinkedHashSet<>());
            } else if (CollectionUtils.isNotEmpty(tableNode.getTableNames())) {
                tableNode.getTableNames().removeIf(t -> !tableNames.contains(t.getOriginTableName()));
            }

            if (CollectionUtils.isNotEmpty(tableNode.getTableNames())) {
                renameMap = tableNode.getTableNames().stream()
                        .collect(Collectors.toMap(TableRenameTableInfo::getOriginTableName, TableRenameTableInfo::getCurrentTableName, (e1,e2)->e1));
            }

            nodeCheckData(tableNode.successors(), tableNames, renameMap);

        } else if (node instanceof MigrateFieldRenameProcessorNode) {
            MigrateFieldRenameProcessorNode fieldNode = (MigrateFieldRenameProcessorNode) node;
            LinkedList<TableFieldInfo> fieldsMapping = fieldNode.getFieldsMapping();
            if (CollectionUtils.isEmpty(tableNames)) {
                fieldNode.setFieldsMapping(new LinkedList<>());
            } else if (CollectionUtils.isNotEmpty(fieldsMapping)) {
                fieldsMapping.removeIf(t -> !tableNames.contains(t.getOriginTableName()));
            }

            if (Objects.nonNull(renameMap) && !renameMap.isEmpty() && CollectionUtils.isNotEmpty(fieldsMapping)) {
                for (TableFieldInfo info : fieldsMapping) {
                    if (renameMap.containsKey(info.getOriginTableName())) {
                        String rename = renameMap.get(info.getOriginTableName());
                        if (!StringUtils.equals(info.getPreviousTableName(), rename)) {
                            info.setPreviousTableName(rename);
                        }
                    }
                }
            }

            nodeCheckData(fieldNode.successors(), tableNames, null);
        }
    }
}

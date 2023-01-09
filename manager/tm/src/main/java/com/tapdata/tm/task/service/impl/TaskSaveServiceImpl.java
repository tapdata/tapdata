package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskSaveServiceImpl implements TaskSaveService {
    private TaskService taskService;
    private TaskDagCheckLogService taskDagCheckLogService;
    private MetadataInstancesService metadataInstancesService;
    private AlarmSettingService alarmSettingService;
    private AlarmRuleService alarmRuleService;

    @Override
    public boolean taskSaveCheckLog(TaskDto taskDto, UserDetail userDetail) {
        taskDagCheckLogService.removeAllByTaskId(taskDto.getId().toHexString());

        boolean noPass = false;
        List<TaskDagCheckLog> taskDagCheckLogs = taskDagCheckLogService.dagCheck(taskDto, userDetail, true);
        if (CollectionUtils.isNotEmpty(taskDagCheckLogs)) {
            Optional<TaskDagCheckLog> any = taskDagCheckLogs.stream().filter(log -> Level.ERROR.equals(log.getGrade())).findAny();
            if (any.isPresent()) {
                noPass = true;
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
        if (Objects.isNull(dag) || org.apache.commons.collections4.CollectionUtils.isEmpty(dag.getNodes())) {
            return;
        }

        //supplier migrate tableSelectType=all tableNames and SyncObjects
        if (CollectionUtils.isNotEmpty(dag.getSourceNode())) {
            DatabaseNode sourceNode = dag.getSourceNode().getFirst();
            List<String> tableNames = sourceNode.getTableNames();
            if (CollectionUtils.isEmpty(tableNames) && StringUtils.equals("expression", sourceNode.getMigrateTableSelectType())) {
                String connectionId = sourceNode.getConnectionId();
                List<MetadataInstancesDto> metaList = metadataInstancesService.findBySourceIdAndTableNameListNeTaskId(connectionId, null, userDetail);
                if (CollectionUtils.isNotEmpty(metaList)) {
                    List<String> collect = metaList.stream()
                            .map(MetadataInstancesDto::getOriginalName)
                            .filter(originalName -> Pattern.matches(sourceNode.getExpression(), originalName))
                            .collect(Collectors.toList());
                    sourceNode.setTableNames(collect);
                }
            }

            nodeCheckData(sourceNode.successors(), tableNames, null);

            Dag temp = new Dag(dag.getEdges(), dag.getNodes());
            DAG.build(temp);
        }

    }

    @Override
    public void supplementAlarm(TaskDto taskDto, UserDetail userDetail) {
        List<AlarmSettingDto> settingDtos = alarmSettingService.findAll();
        List<AlarmRuleDto> ruleDtos = alarmRuleService.findAll();

        Map<AlarmKeyEnum, AlarmSettingDto> settingDtoMap = settingDtos.stream().collect(Collectors.toMap(AlarmSettingDto::getKey, Function.identity(), (e1, e2) -> e1));
        Map<AlarmKeyEnum, AlarmRuleDto> ruleDtoMap = ruleDtos.stream().collect(Collectors.toMap(AlarmRuleDto::getKey, Function.identity(), (e1, e2) -> e1));

        List<AlarmSettingDto> alarmSettingDtos = Lists.newArrayList();
        List<AlarmRuleDto> alarmRuleDtos = Lists.newArrayList();
        if (CollectionUtils.isEmpty(taskDto.getAlarmSettings())) {
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_STATUS_ERROR));
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_INSPECT_ERROR));
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_FULL_COMPLETE));
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_INCREMENT_START));
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_STATUS_STOP));
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_INCREMENT_DELAY));

            alarmRuleDtos.add(ruleDtoMap.get(AlarmKeyEnum.TASK_INCREMENT_DELAY));

            taskDto.setAlarmSettings(alarmSettingDtos);
            taskDto.setAlarmRules(alarmRuleDtos);
        }

        if (Objects.nonNull(taskDto.getDag()) && CollectionUtils.isNotEmpty(taskDto.getDag().getNodes())) {
            for (Node<?> node : taskDto.getDag().getNodes()) {
                alarmSettingDtos = Lists.newArrayList();
                alarmRuleDtos = Lists.newArrayList();
                if (node != null) {
                    if (node.isDataNode()) {
                        if (CollectionUtils.isEmpty(node.getAlarmSettings())) {
//                            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.DATANODE_CANNOT_CONNECT));
                            //alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.DATANODE_HTTP_CONNECT_CONSUME));
                            //alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.DATANODE_TCP_CONNECT_CONSUME));
                            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME));
                        }

                        if (CollectionUtils.isEmpty(node.getAlarmRules())) {
                            //alarmRuleDtos.add(ruleDtoMap.get(AlarmKeyEnum.DATANODE_HTTP_CONNECT_CONSUME));
                            //alarmRuleDtos.add(ruleDtoMap.get(AlarmKeyEnum.DATANODE_TCP_CONNECT_CONSUME));
                            alarmRuleDtos.add(ruleDtoMap.get(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME));
                        }

                    } else {

                        if (CollectionUtils.isEmpty(node.getAlarmSettings())) {
                            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME));
                        }

                        if (CollectionUtils.isEmpty(node.getAlarmRules())) {
                            alarmRuleDtos.add(ruleDtoMap.get(AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME));
                        }
                    }
                    if (CollectionUtils.isNotEmpty(alarmSettingDtos)) {
                        node.setAlarmSettings(alarmSettingDtos);
                    }
                    if (CollectionUtils.isNotEmpty(alarmRuleDtos)) {
                        node.setAlarmRules(alarmRuleDtos);
                    }
                }
            }

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

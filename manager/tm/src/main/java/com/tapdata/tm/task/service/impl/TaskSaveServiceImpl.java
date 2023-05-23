package com.tapdata.tm.task.service.impl;

import cn.hutool.extra.cglib.CglibUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleVO;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import io.tapdata.entity.conversion.PossibleDataTypes;
import io.tapdata.entity.schema.type.TapType;
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
    private MetadataInstancesService metadataInstancesService;
    private AlarmSettingService alarmSettingService;
    private AlarmRuleService alarmRuleService;

    @Override
    public void syncTaskSetting(TaskDto taskDto, UserDetail userDetail) {
        DAG dag = taskDto.getDag();
        if (Objects.isNull(dag) || org.apache.commons.collections4.CollectionUtils.isEmpty(dag.getNodes())) {
            return;
        }

        List<MetadataInstancesDto> schemaList = metadataInstancesService.findByTaskId(taskDto.getId().toHexString(), userDetail);
        if (CollectionUtils.isEmpty(schemaList)) {
            return;
        }

        Map<String, List<MetadataInstancesDto>> schemaMap = schemaList.stream().collect(Collectors.groupingBy(MetadataInstancesDto::getNodeId));

        boolean needBuildDag = false;
        for (Node node : dag.getNodes()) {
            Map<String, List<String>> notSupportFieldMap = Maps.newHashMap();
            List<MetadataInstancesDto> metadataInstancesDtos = schemaMap.get(node.getId());
            if (CollectionUtils.isEmpty(metadataInstancesDtos)) {
                continue;
            }

            for (MetadataInstancesDto instancesDto : metadataInstancesDtos) {

                List<String> notSupportFields = Lists.newArrayList();
                if (Node.SourceType.source == node.sourceType()) {
                    instancesDto.getFields().forEach(k -> {
                        TapType tapType = JSON.parseObject(k.getTapType(), TapType.class);
                        if (TapType.TYPE_RAW == tapType.getType()) {
                            notSupportFields.add(k.getOriginalFieldName());
                        }
                    });

                } else {
                    Map<String, PossibleDataTypes> findPossibleDataTypes = instancesDto.getFindPossibleDataTypes();
                    if (Objects.isNull(findPossibleDataTypes)) {
                        continue;
                    }

                    findPossibleDataTypes.forEach((k, v) -> {
                        if (CollectionUtils.isEmpty(v.getDataTypes())) {
                            notSupportFields.add(k);
                        }
                    });
                }

                if (CollectionUtils.isNotEmpty(notSupportFields)) {
                    notSupportFieldMap.put(instancesDto.getOriginalName(), notSupportFields);
                }
            }

            if (!notSupportFieldMap.isEmpty()) {
                node.setNotSupportFieldMap(notSupportFieldMap);
                needBuildDag = true;
            }
        }

        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            if (needBuildDag) {
                Dag temp = new Dag(dag.getEdges(), dag.getNodes());
                DAG.build(temp);
            }
            return;
        }

        //supplier migrate tableSelectType=all tableNames and SyncObjects
        if (CollectionUtils.isNotEmpty(dag.getSourceNode())) {
            dag.getSourceNode().forEach(sourceNode -> {
                if (StringUtils.equals("expression", sourceNode.getMigrateTableSelectType())) {
                    String connectionId = sourceNode.getConnectionId();
                    List<MetadataInstancesDto> metaList = metadataInstancesService.findBySourceIdAndTableNameListNeTaskId(connectionId, null, userDetail);
                    if (CollectionUtils.isNotEmpty(metaList)) {
                        List<String> collect = metaList.stream()
                                .map(MetadataInstancesDto::getOriginalName)
                                .filter(originalName -> {
                                    if (StringUtils.isEmpty(sourceNode.getTableExpression())) {
                                        return false;
                                    } else {
                                        return Pattern.matches(sourceNode.getTableExpression(), originalName);
                                    }
                                })
                                .collect(Collectors.toList());
                        sourceNode.setTableNames(collect);
                    }
                }

                nodeCheckData(sourceNode.successors(), sourceNode.getTableNames(), null);
            });

            if (CollectionUtils.isNotEmpty(dag.getTargets())) {
                dag.getTargets().stream()
                        .filter(node -> node instanceof DatabaseNode)
                        .forEach(target -> {
                            DatabaseNode databaseNode = (DatabaseNode) target;
                            if (Objects.isNull(databaseNode.getUpdateConditionFieldMap())) {
                                databaseNode.setUpdateConditionFieldMap(Maps.newHashMap());
                            }

                            String nodeId = databaseNode.getId();
                            long updateExNum = metadataInstancesService.countUpdateExNum(nodeId);
                            if (updateExNum > 0) {
                                List<MetadataInstancesDto> metaList = schemaMap.get(nodeId);
                                Optional.ofNullable(metaList).ifPresent(list -> {
                                    list.forEach(schema -> {
                                        List<String> fields = schema.getFields().stream().filter(Field::getPrimaryKey).map(Field::getFieldName).collect(Collectors.toList());
                                        if (CollectionUtils.isNotEmpty(fields)) {
                                            databaseNode.getUpdateConditionFieldMap().put(schema.getName(), fields);
                                        } else {
                                            List<String> columnList = schema.getIndices() == null ? null : schema.getIndices().stream().filter(TableIndex::isUnique)
                                                    .flatMap(idc -> idc.getColumns().stream())
                                                    .map(TableIndexColumn::getColumnName)
                                                    .collect(Collectors.toList());
                                            if (CollectionUtils.isNotEmpty(columnList)) {
                                                databaseNode.getUpdateConditionFieldMap().put(schema.getName(), columnList);
                                            }
                                        }
                                    });
                                });
                            }
                        });
            }

            Dag temp = new Dag(dag.getEdges(), dag.getNodes());
            DAG.build(temp);
        }
    }

    @Override
    public void supplementAlarm(TaskDto taskDto, UserDetail userDetail) {
        List<AlarmSettingDto> settingDtos = alarmSettingService.findAllAlarmSetting(userDetail);
        List<AlarmRuleDto> ruleDtos = alarmRuleService.findAllAlarm(userDetail);

        Map<AlarmKeyEnum, AlarmSettingDto> settingDtoMap = settingDtos.stream().collect(Collectors.toMap(AlarmSettingDto::getKey, Function.identity(), (e1, e2) -> e1));
        Map<AlarmKeyEnum, AlarmRuleDto> ruleDtoMap = ruleDtos.stream().collect(Collectors.toMap(AlarmRuleDto::getKey, Function.identity(), (e1, e2) -> e1));

        List<AlarmSettingDto> alarmSettingDtos = Lists.newArrayList();
        List<AlarmRuleDto> alarmRuleDtos = Lists.newArrayList();
        if (CollectionUtils.isEmpty(taskDto.getAlarmSettings())) {
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_STATUS_ERROR));
            //alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_INSPECT_ERROR));
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_FULL_COMPLETE));
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_INCREMENT_START));
//            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_STATUS_STOP));
            alarmSettingDtos.add(settingDtoMap.get(AlarmKeyEnum.TASK_INCREMENT_DELAY));
            taskDto.setAlarmSettings(CglibUtil.copyList(alarmSettingDtos, AlarmSettingVO::new));
        }

        if (CollectionUtils.isEmpty(taskDto.getAlarmRules())) {
            alarmRuleDtos.add(ruleDtoMap.get(AlarmKeyEnum.TASK_INCREMENT_DELAY));
            taskDto.setAlarmRules(CglibUtil.copyList(alarmRuleDtos, AlarmRuleVO::new));
        }

        if (Objects.nonNull(taskDto.getDag()) && CollectionUtils.isNotEmpty(taskDto.getDag().getNodes())) {
            for (Node<?> node : taskDto.getDag().getNodes()) {
                List<AlarmSettingDto> nodeSettings = Lists.newArrayList();
                List<AlarmRuleDto> nodeRules = Lists.newArrayList();
                FunctionUtils.isTureOrFalse(node.isDataNode()).trueOrFalseHandle(() -> {
                            if (CollectionUtils.isEmpty(node.getAlarmSettings())) {
                                nodeSettings.add(settingDtoMap.get(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME));
                                node.setAlarmSettings(CglibUtil.copyList(nodeSettings, AlarmSettingVO::new));
                            }
                            if (CollectionUtils.isEmpty(node.getAlarmRules())) {
                                nodeRules.add(ruleDtoMap.get(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME));
                                node.setAlarmRules(CglibUtil.copyList(nodeRules, AlarmRuleVO::new));
                            }
                        },
                        () -> {
                            if (CollectionUtils.isEmpty(node.getAlarmSettings())) {
                                nodeSettings.add(settingDtoMap.get(AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME));
                                node.setAlarmSettings(CglibUtil.copyList(nodeSettings, AlarmSettingVO::new));
                            }
                            if (CollectionUtils.isEmpty(node.getAlarmRules())) {
                                nodeRules.add(ruleDtoMap.get(AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME));
                                node.setAlarmRules(CglibUtil.copyList(nodeRules, AlarmRuleVO::new));
                            }
                        });
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

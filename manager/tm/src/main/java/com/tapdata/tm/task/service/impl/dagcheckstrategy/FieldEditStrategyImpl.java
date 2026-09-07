package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import com.tapdata.tm.commons.dag.process.FieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Component("fieldEditStrategy")
public class FieldEditStrategyImpl implements DagLogStrategy {

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.FIELD_EDIT_NODE_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
        String taskId = taskDto.getId().toHexString();

        Date now = new Date();
        String userId = userDetail.getUserId();
        List<TaskDagCheckLog> result = Lists.newArrayList();
        DAG dag = taskDto.getDag();

        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getNodes())) {
            return null;
        }

        dag.getNodes().stream()
                .filter(node -> node instanceof MigrateFieldRenameProcessorNode || node instanceof FieldRenameProcessorNode)
                .forEach(node -> {
                    String name = node.getName();
                    String nodeId = node.getId();

                    if (StringUtils.isEmpty(name)) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.ERROR, templateEnum, locale, "FIELD_EDIT_NAME_EMPTY"));
                    }

                    AtomicBoolean renameEmpty = new AtomicBoolean(false);
                    AtomicReference<String> fieldName = new AtomicReference<>("");
                    if (node instanceof MigrateFieldRenameProcessorNode) {
                        LinkedList<TableFieldInfo> fieldsMapping = ((MigrateFieldRenameProcessorNode) node).getFieldsMapping();
                        Optional.ofNullable(fieldsMapping).ifPresent(list -> {
                            for (TableFieldInfo info : list) {
                                if (CollectionUtils.isNotEmpty(info.getFields())) {
                                    for (FieldInfo field : info.getFields()) {
                                        if (StringUtils.isEmpty(field.getTargetFieldName())) {
                                            renameEmpty.set(true);
                                            fieldName.set(field.getSourceFieldName());
                                            break;
                                        }
                                    }

                                    if (renameEmpty.get()) {
                                        break;
                                    }
                                }

                            }
                        });
                    } else {
                        List<FieldProcessorNode.Operation> operations = ((FieldRenameProcessorNode) node).getOperations();
                        Optional.ofNullable(operations).ifPresent(list -> {
                            for (FieldProcessorNode.Operation operation : list) {
                                if (StringUtils.isNotBlank(operation.getOp()) && StringUtils.isBlank(operation.getField())) {
                                    renameEmpty.set(true);
                                    fieldName.set(operation.getField());
                                    break;
                                }
                            }
                        });
                    }
                    if (renameEmpty.get()) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.ERROR, templateEnum, locale, "FIELD_EDIT_FIELD_EMPTY", name, fieldName.get()));
                    }


                    if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                        result.add(DagCheckLogs.of(taskId, nodeId, userId, now, Level.INFO, templateEnum, locale, "FIELD_EDIT_PASS", name));
                    }
                });

        return result;
    }
}

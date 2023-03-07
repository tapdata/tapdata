package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import com.tapdata.tm.commons.dag.process.FieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
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
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.ERROR).nodeId(nodeId)
                                .log(MessageUtil.getDagCheckMsg(locale, "FIELD_EDIT_NAME_EMPTY"))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
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
                                if (StringUtils.isNotBlank(operation.getOp()) && StringUtils.isBlank(operation.getLabel())) {
                                    renameEmpty.set(true);
                                    fieldName.set(operation.getField());
                                    break;
                                }
                            }
                        });
                    }
                    if (renameEmpty.get()) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.ERROR).nodeId(nodeId)
                                .log(MessageFormat.format(MessageUtil.getDagCheckMsg(locale, "FIELD_EDIT_FIELD_EMPTY"), name, fieldName.get()))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }


                    if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.INFO).nodeId(nodeId)
                                .log(MessageFormat.format(MessageUtil.getDagCheckMsg(locale, "FIELD_EDIT_PASS"), name))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }
                });

        return result;
    }
}

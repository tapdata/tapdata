package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.FieldAddDelProcessorNode;
import com.tapdata.tm.commons.dag.process.FieldCalcProcessorNode;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

@Component("fieldAddDelStrategy")
@Setter(onMethod_ = {@Autowired})
public class FieldAddDelStrategyImpl implements DagLogStrategy {

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.FIELD_ADD_DEL_CHECK;

    private MetadataInstancesService metadataInstancesService;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            return null;
        }

        DAG dag = taskDto.getDag();
        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getNodes())) {
            return null;
        }

        String taskId = taskDto.getId().toHexString();
        Date now = new Date();
        String userId = userDetail.getUserId();
        List<TaskDagCheckLog> result = Lists.newArrayList();
        dag.getNodes().stream()
                .filter(node -> node instanceof FieldAddDelProcessorNode)
                .map(node -> (FieldAddDelProcessorNode) node)
                .forEach(node -> {
                    String name = node.getName();
                    String nodeId = node.getId();

                    if (StringUtils.isEmpty(name)) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.ERROR).nodeId(nodeId)
                                .log("$date【$taskName】【增删字段节点检测】：增删字段节点节点名称为空。")
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }

                    List<MetadataInstancesDto> metadataInstancesDtos = metadataInstancesService.findByNodeId(nodeId, userDetail);
                    boolean fieldEmpty = false;
                    if (CollectionUtils.isNotEmpty(metadataInstancesDtos)) {
                        List<FieldProcessorNode.Operation> operations = node.getOperations();
                        if (CollectionUtils.isNotEmpty(operations)) {
                            Map<String, String> collect = operations.stream()
                                    .collect(Collectors.toMap(FieldProcessorNode.Operation::getField, FieldProcessorNode.Operation::getOp, (e1, e2) -> e1));

                            List<String> fields = metadataInstancesDtos.get(0).getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
                            fields.removeIf(field -> collect.containsKey(field) && collect.get(field).equals("REMOVE"));
                            if ((node.isDeleteAllFields() || CollectionUtils.isEmpty(fields)) && !collect.containsValue("CREATE")) {
                                fieldEmpty = true;
                            }
                        }
                    }

                    if (fieldEmpty) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.ERROR).nodeId(nodeId)
                                .log(MessageFormat.format("$date【$taskName】【增删字段节点检测】：增删字段节点{0}字段被全部删除。", name))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }


                    if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.INFO).nodeId(nodeId)
                                .log(MessageFormat.format("$date【$taskName】【增删字段节点检测】：增删字段节点{0}检测通过。", name))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }
                });

        return result;
    }
}

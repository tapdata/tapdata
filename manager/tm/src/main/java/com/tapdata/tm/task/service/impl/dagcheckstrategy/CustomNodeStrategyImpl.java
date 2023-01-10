package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.customNode.dto.CustomNodeDto;
import com.tapdata.tm.customNode.service.CustomNodeService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component("customNodeStrategy")
@Setter(onMethod_ = {@Autowired})
public class CustomNodeStrategyImpl implements DagLogStrategy {

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.CUSTOM_NODE_CHECK;

    private CustomNodeService customNodeService;

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
                .filter(node -> node instanceof CustomProcessorNode)
                .forEach(node -> {
                    CustomProcessorNode customProcessorNode = (CustomProcessorNode) node;
                    String name = node.getName();
                    String nodeId = node.getId();
                    String customNodeId = customProcessorNode.getCustomNodeId();

                    CustomNodeDto customNodeDto = customNodeService.findById(new ObjectId(customNodeId));
                    if (Objects.isNull(customNodeDto)) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.ERROR).nodeId(nodeId)
                                .log("$date【$taskName】【自定义节点检测】：自定义节点节点不存在。")
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    } else {
                        List<String> formRequired = Lists.newArrayList();
                        Map<String, Object> formSchema = customNodeDto.getFormSchema();
                        if (!formSchema.isEmpty()) {
                            Map schema = (Map) formSchema.get("schema");
                            if (!schema.isEmpty()) {
                                Map properties = (Map) schema.get("properties");
                                if (!properties.isEmpty()) {
                                    for (Object o : properties.keySet()) {
                                        Map map = (Map) properties.get(o.toString());
                                        boolean required = (Boolean) map.getOrDefault("required", false);
                                        formRequired.add(o.toString());
                                    }
                                }
                            }
                        }

                        if (CollectionUtils.isNotEmpty(formRequired)) {
                            boolean requiredFlag = false;
                            String formName = "";
                            Map<String, Object> form = ((CustomProcessorNode) node).getForm();
                            for (String key : formRequired) {
                                if (!form.containsKey(key)) {
                                    requiredFlag = true;
                                    formName = key;
                                }
                            }

                            if (requiredFlag) {
                                TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                        .grade(Level.ERROR).nodeId(nodeId)
                                        .log(MessageFormat.format("$date【$taskName】【自定义节点检测】：{0}的设置{1}为空。", name, formName))
                                        .build();
                                log.setCreateAt(now);
                                log.setCreateUser(userId);
                                result.add(log);
                            }
                        }
                    }

                    if (CollectionUtils.isEmpty(result) || result.stream().anyMatch(log -> nodeId.equals(log.getNodeId()))) {
                        TaskDagCheckLog log = TaskDagCheckLog.builder().taskId(taskId).checkType(templateEnum.name())
                                .grade(Level.INFO).nodeId(nodeId)
                                .log(MessageFormat.format("$date【$taskName】【自定义节点检测】：自定义节点{0}检测通过。", name))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }
                });

        return result;
    }
}

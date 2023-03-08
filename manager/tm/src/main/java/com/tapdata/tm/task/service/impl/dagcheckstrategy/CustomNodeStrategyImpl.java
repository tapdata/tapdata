package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.customNode.dto.CustomNodeDto;
import com.tapdata.tm.customNode.service.CustomNodeService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;

@Component("customNodeStrategy")
@Setter(onMethod_ = {@Autowired})
public class CustomNodeStrategyImpl implements DagLogStrategy {

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.CUSTOM_NODE_CHECK;

    private CustomNodeService customNodeService;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
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
                                .log(MessageUtil.getDagCheckMsg(locale, "CUSTOM_NODE_NOT_EXISTS"))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    } else {
                        List<String> formRequired = Lists.newArrayList();
                        Map<String, Object> formSchema = customNodeDto.getFormSchema();

                        FunctionUtils.ignoreAnyError(() -> {
                            Map properties = JsonUtil.getValue(formSchema, "schema.properties", Map.class);
                            Optional.ofNullable(properties).ifPresent(prop -> {
                                prop.forEach((key, value) -> {
                                    Map formMap = (Map) value;
                                    if ((Boolean) formMap.getOrDefault("required", false)) {
                                        formRequired.add(key.toString());
                                    }
                                });
                            });
                        });

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
                                        .log(MessageFormat.format(MessageUtil.getDagCheckMsg(locale, "CUSTOM_NODE_SET_EMPTY"), name, formName))
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
                                .log(MessageFormat.format(MessageUtil.getDagCheckMsg(locale, "CUSTOM_NODE_SET_EMPTY"), name))
                                .build();
                        log.setCreateAt(now);
                        log.setCreateUser(userId);
                        result.add(log);
                    }
                });

        return result;
    }
}

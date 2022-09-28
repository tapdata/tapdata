package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Component("agentStrategy")
@Setter(onMethod_ = {@Autowired})
public class AgentStrategyImpl implements DagLogStrategy {

    private WorkerService workerService;

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.AGENT_CAN_USE_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {

        List<Worker> availableAgent;
        String agent;
        if (StringUtils.equals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), taskDto.getAccessNodeType())) {
            availableAgent = workerService.findAvailableAgent(userDetail);
            agent = AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.getName();
        } else {
            availableAgent = workerService.findAvailableAgentByAccessNode(userDetail, taskDto.getAccessNodeProcessIdList());
            agent = taskDto.getAccessNodeProcessIdList().get(0);
        }

        String template;
        String content;
        Level grade;
        if (CollectionUtils.isNotEmpty(availableAgent)) {
            template = templateEnum.getInfoTemplate();
            List<String> collect = availableAgent.stream().map(Worker::getHostname).collect(Collectors.toList());
            content = MessageFormat.format(template, DateUtil.now(), availableAgent.size(), StringUtils.join(collect, ","),
                    availableAgent.get(0).getHostname());
            grade = Level.INFO;
        } else {
            template = templateEnum.getErrorTemplate();
            content = MessageFormat.format(template, DateUtil.now(), agent);
            grade = Level.ERROR;
        }

        TaskDagCheckLog log = new TaskDagCheckLog();
        log.setTaskId(taskDto.getId().toHexString());
        log.setCheckType(templateEnum.name());
        log.setCreateAt(new Date());
        log.setCreateUser(userDetail.getUserId());
        log.setLog(content);
        log.setGrade(grade);

        return Lists.newArrayList(log);
    }
}

package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Component("agentStrategy")
@Setter(onMethod_ = {@Autowired})
public class AgentStrategyImpl implements DagLogStrategy {

    private WorkerService workerService;
    private AgentGroupService agentGroupService;

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.AGENT_CAN_USE_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale) {
        AtomicReference<String> agentRef = new AtomicReference<>();
        List<Worker> availableAgent = getWorkers(taskDto, userDetail, agentRef);
        String agent = agentRef.get();

        String template;
        String content;
        Level grade;
        if (CollectionUtils.isNotEmpty(availableAgent)) {
            template = MessageUtil.getDagCheckMsg(locale, "AGENT_CAN_USE_INFO");
            List<String> collect = availableAgent.stream().map(Worker::getHostname).collect(Collectors.toList());
            content = MessageFormat.format(template, DateUtil.now(), availableAgent.size(), StringUtils.join(collect, ","),
                    availableAgent.get(0).getHostname());
            grade = Level.INFO;
        } else {
            template = MessageUtil.getDagCheckMsg(locale, "AGENT_CAN_USE_ERROR");
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

    protected List<Worker> getWorkers(TaskDto taskDto, UserDetail userDetail, AtomicReference<String> agent) {
        List<Worker> availableAgent;
        if (StringUtils.equals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), taskDto.getAccessNodeType())) {
            availableAgent = workerService.findAvailableAgent(userDetail);
            agent.set(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.getName());
        } else {
            List<String> processNodeListWithGroup = agentGroupService.getProcessNodeListWithGroup(taskDto, userDetail);
            availableAgent = workerService.findAvailableAgentByAccessNode(userDetail, processNodeListWithGroup);
            if (null != availableAgent && !availableAgent.isEmpty()) {
                agent.set(processNodeListWithGroup.get(0));
            } else {
                agent.set("");
            }
        }
        return availableAgent;
    }
}

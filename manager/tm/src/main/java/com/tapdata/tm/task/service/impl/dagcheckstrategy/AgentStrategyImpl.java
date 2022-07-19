package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.common.sample.sampler.AverageSampler;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

@Component("agentStrategy")
@Setter(onMethod_ = {@Autowired})
public class AgentStrategyImpl implements DagLogStrategy {

    private WorkerService workerService;

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.AGENT_CAN_USE_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {

        List<Worker> availableAgent;
        if (StringUtils.equals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), taskDto.getAccessNodeType())) {
            availableAgent = workerService.findAvailableAgent(userDetail);
        } else {
            availableAgent = workerService.findAvailableAgentByAccessNode(userDetail, taskDto.getAccessNodeProcessIdList());
        }

        String template;
        String content;
        if (CollectionUtils.isNotEmpty(availableAgent)) {
            template = templateEnum.getInfoTemplate();
            content = String.format(template, DateUtil.now(), taskDto.getName(), availableAgent.size(), availableAgent.get(0).getHostname());
        } else {
            template = templateEnum.getErrorTemplate();
            content = String.format(template, DateUtil.now(), taskDto.getName());
        }

        TaskDagCheckLog log = new TaskDagCheckLog();
        log.setTaskId(taskDto.getId().toHexString());
        log.setCheckType(templateEnum.name());
        log.setCreateAt(new Date());
        log.setCreateUser(userDetail.getUserId());
        log.setLog(content);

        return Lists.newArrayList(log);
    }
}

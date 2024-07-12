package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InspectCronJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        InspectService inspectService = SpringContextHelper.getBean(InspectService.class);
        InspectTaskService inspectTaskService = SpringContextHelper.getBean(InspectTaskService.class);
        UserService userService = SpringContextHelper.getBean(UserService.class);

        JobKey jobKey = jobExecutionContext.getJobDetail().getKey();
        String inspectId = jobKey.getName();
        log.info("工作任务的名称:" + inspectId + " 工作任务组的名称:" + jobKey.getGroup());
        InspectDto inspectDto = inspectService.findById(MongoUtils.toObjectId(inspectId));

        String status = inspectDto.getStatus();
        if (InspectStatusEnum.SCHEDULING.getValue().equals(status) || InspectStatusEnum.RUNNING.getValue().equals(status)) {
            log.info("inspect {},status:{}  不用在进行校验", inspectId, status);
        } else {
            log.info("inspect {},status:{}  定时在进行校验", inspectId, status);
            UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(inspectDto.getUserId()));
            Where where = new Where();
            where.put("id", inspectId);
            inspectTaskService.executeInspect(where, inspectDto, userDetail);
        }
    }
}

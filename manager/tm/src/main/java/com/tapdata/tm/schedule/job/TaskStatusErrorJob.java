package com.tapdata.tm.schedule.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component("taskStatusErrorJob")
public class TaskStatusErrorJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

    }
}

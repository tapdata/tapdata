package com.tapdata.tm.utils;

import com.tapdata.tm.schedule.entity.ScheduleJobInfo;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Component;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class QuartzManager {
    private SchedulerFactoryBean schedulerFactoryBean;
    public static final String TRIGGER_GROUP_NAME = "EXTJWEB_TRIGGERGROUP_NAME";

    /**
     * 添加任务，使用任务组名（不存在就用默认的），触发器名，触发器组名并启动
     * @param info 任务
     */
    @SuppressWarnings("unchecked")
    public void addJob(ScheduleJobInfo info, JobDataMap map) {
        try {
            Scheduler scheduler = schedulerFactoryBean.getScheduler();

            JobKey jobKey = JobKey.jobKey(info.getJobName(), info.getGroupName());
            JobDetail jobDetail = scheduler.getJobDetail(jobKey);
            if (null != jobDetail) {
                log.info("{}， {} 定时任务已经存在", info.getJobName(), info.getGroupName());
                return;
            }

            // JobDetail 是具体Job实例
            jobDetail = JobBuilder.newJob((Class<? extends Job>) Class.forName(info.getClassName()))
                    .withIdentity(info.getJobName(),info.getGroupName())
                    .usingJobData(map)
                    .build();

            // 基于表达式构建触发器
            CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(info.getCron());
            // CronTrigger表达式触发器 继承于Trigger
            // TriggerBuilder 用于构建触发器实例
            CronTrigger cronTrigger = TriggerBuilder.newTrigger().withIdentity(info.getJobName()+"_trigger", TRIGGER_GROUP_NAME)
                    .withSchedule(cronScheduleBuilder).build();

            scheduler.scheduleJob(jobDetail, cronTrigger);

            //启动
            if (!scheduler.isShutdown()) {
                scheduler.start();
            }
        } catch (SchedulerException | ClassNotFoundException e) {
            log.error("添加任务失败",e);
        }
    }

    /**
     * 暂停任务
     * @param info
     */
    public void pauseJob(ScheduleJobInfo info) {
        try{
            Scheduler scheduler = schedulerFactoryBean.getScheduler();
            JobKey jobKey = JobKey.jobKey(info.getJobName(), info.getGroupName());
            scheduler.pauseJob(jobKey);
            log.info("=========================pause job: {} success========================", info.getJobName());
        }catch (Exception e){
            log.error("",e);
        }
    }

    /**
     * 恢复任务
     * @param info
     */
    public void resumeJob(ScheduleJobInfo info) {
        try {
            Scheduler scheduler = schedulerFactoryBean.getScheduler();
            JobKey jobKey = JobKey.jobKey(info.getJobName(), info.getGroupName());
            scheduler.resumeJob(jobKey);
            log.info("=========================resume job: {} success========================", info.getJobName());
        }catch (Exception e){
            log.error("",e);
        }
    }

    /**
     * 删除任务，在业务逻辑中需要更新库表的信息
     * @param info
     * @return
     */
    public boolean removeJob(ScheduleJobInfo info) {
        boolean result = true;
        try {
            Scheduler scheduler = schedulerFactoryBean.getScheduler();
            JobKey jobKey = JobKey.jobKey(info.getJobName(), info.getGroupName());
            JobDetail jobDetail = scheduler.getJobDetail(jobKey);
            if (null != jobDetail) {
                result =  scheduler.deleteJob(jobKey);
            }
            log.info("=========================remove job: {} {}========================", info.getJobName(), result);
        }catch (Exception e){
            log.error("",e);
            result=false;
        }
        return result;
    }

    /**
     * 修改定时任务的时间
     * @param info
     * @return
     */
    public boolean modifyJobTime(ScheduleJobInfo info){
        boolean result = true;
        try{
            Scheduler scheduler = schedulerFactoryBean.getScheduler();
            TriggerKey triggerKey = TriggerKey.triggerKey(info.getJobName() + "_trigger", TRIGGER_GROUP_NAME);
            CronTrigger trigger = (CronTrigger)scheduler.getTrigger(triggerKey);

            String oldTime = trigger.getCronExpression();
            if (!StringUtils.equalsIgnoreCase(oldTime, info.getCron())) {
                CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(info.getCron());
                CronTrigger ct = TriggerBuilder.newTrigger().withIdentity(info.getJobName()+ RandomStringUtils.randomAlphabetic(6) + "_trigger", TRIGGER_GROUP_NAME)
                        .withSchedule(cronScheduleBuilder)
                        .build();

                scheduler.rescheduleJob(triggerKey, ct);
                scheduler.resumeTrigger(triggerKey);
            }

        }catch (Exception e){
            log.error("", e);
            result=false;
        }
        return result;
    }

    /**
     * 启动所有定时任务
     */
    public void startJobs() {
        try {
            Scheduler scheduler = schedulerFactoryBean.getScheduler();
            scheduler.start();
        } catch (SchedulerException e) {
            log.error("",e);
        }
    }
}

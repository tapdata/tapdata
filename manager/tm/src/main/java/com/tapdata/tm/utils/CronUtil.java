package com.tapdata.tm.utils;

import com.tapdata.tm.inspect.service.InspectCronJob;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Date;

@Slf4j
public class CronUtil {

    private static final String TRIGGER_PRE = "trigger_";

    public static void addJob(Date startDate, Date endDate, Long intervals, String intervalsUnit, String id) {
        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            JobDetail job = JobBuilder.newJob(InspectCronJob.class).withIdentity(id).build();
            if (scheduler.checkExists(job.getKey())){
                log.info("id:  {}  job 定时任务已经存在，不再重复设置 ",id);
                return;
            }

            SimpleScheduleBuilder simpleScheduleBuilder = null;
            if ("second".equals(intervalsUnit)) {
                simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(intervals.intValue()).repeatForever();
            } else if ("minute".equals(intervalsUnit)) {
                simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInMinutes(intervals.intValue()).repeatForever();
            } else if ("hour".equals(intervalsUnit)) {
                simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInHours(intervals.intValue()).repeatForever();
            } else if ("day".equals(intervalsUnit)) {
                Integer dateIntervals = Math.toIntExact(intervals * 24);
                simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInHours(dateIntervals).repeatForever();
            } else if ("week".equals(intervalsUnit)) {
                Integer weekIntervals = Math.toIntExact(intervals * 24) * 7;
                simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInHours(weekIntervals).repeatForever();
            } else if ("month".equals(intervalsUnit)) {
                Integer monthIntervals = Math.toIntExact(intervals * 24) * 7 * 30;
                simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInHours(monthIntervals).repeatForever();
            }

            String triggerName = TRIGGER_PRE + id;
            SimpleTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName)
                    .startAt(startDate).endAt(endDate)
                    .withSchedule(
                            simpleScheduleBuilder
                    ).build();

            scheduler.scheduleJob(job, trigger);
            // 调度启动
            scheduler.start();
        } catch (Exception e) {
            log.error("设置定时任务异常", e);
        }
    }


    /**
     * @Description: 移除一个任务
     */
    public static void removeJob(String id) {
        String triggerName = TRIGGER_PRE + id;
        try {
            TriggerKey triggerKey = TriggerKey.triggerKey(triggerName);
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            Trigger abc = scheduler.getTrigger(triggerKey);
            scheduler.pauseTrigger(triggerKey);// 停止触发器
            scheduler.unscheduleJob(triggerKey);// 移除触发器
            scheduler.deleteJob(JobKey.jobKey(id));// 删除任务
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("id:{}  的定时校验被移除", id);
    }


    /**
     * @param jobName
     * @param jobGroupName
     * @param triggerName      触发器名
     * @param triggerGroupName 触发器组名
     * @param cron             时间设置，参考quartz说明文档
     * @Description: 修改一个任务的触发时间
     */
    @SuppressWarnings("unused")
    public void modifyJobTime(String jobName, String jobGroupName,
                              String triggerName, String triggerGroupName, String cron)
            throws Exception {
        TriggerKey triggerKey = TriggerKey.triggerKey(triggerName,
                triggerGroupName);
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);
        if (trigger == null) {
            return;
        }

        String oldTime = trigger.getCronExpression();
        if (!oldTime.equalsIgnoreCase(cron)) {
            /** 方式一 ：调用 rescheduleJob 开始 */
            // 触发器
            TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder
                    .newTrigger();
            // 触发器名,触发器组
            triggerBuilder.withIdentity(triggerName, triggerGroupName);
            triggerBuilder.startNow();
            // 触发器时间设定
            triggerBuilder.withSchedule(CronScheduleBuilder.cronSchedule(cron));
            // 创建Trigger对象
            trigger = (CronTrigger) triggerBuilder.build();
            // 方式一 ：修改一个任务的触发时间
            scheduler.rescheduleJob(triggerKey, trigger);
            /** 方式一 ：调用 rescheduleJob 结束 */

            /** 方式二：先删除，然后在创建一个新的Job */
            // JobDetail jobDetail =
            // scheduler.getJobDetail(JobKey.jobKey(jobName, jobGroupName));
            // Class<? extends Job> jobClass = jobDetail.getJobClass();
            // removeJob(jobName, jobGroupName, triggerName,
            // triggerGroupName);
            // addJob(jobName, jobGroupName, triggerName, triggerGroupName,
            // jobClass, cron);
            /** 方式二 ：先删除，然后在创建一个新的Job */
        }
    }

    /**
     * @param metadataTaskInfoDTO
     * @desc 设置调度程序的运行频率, 采集周期：0-小时；1-天；2-周；3-月；4-手动采集
     */
  /*  private String getCron(String period, Timing timing) {
        StringBuilder cron = new StringBuilder();
        Long intervals = timing.getIntervals();
        String intervalsUnit = timing.getIntervalsUnit();
        if ("hour".equals(intervalsUnit)) {
            cron.append("0 ").append(" ").append("/")
                    .append(intervals).append(" ").append("* ").append("* ").append("?");
        } else if ("day".equals(intervalsUnit)) {
            cron.append("0 ").append(metadataTaskInfoDTO.getMinuteCollectionTime()).append(" ")
                    .append(metadataTaskInfoDTO.getHourCollectionTime()).append(" ").append("* ").append("* ").append("?");
        } else if ("week".equals(intervalsUnit)) {
            cron.append("0 ").append(metadataTaskInfoDTO.getMinuteCollectionTime()).append(" ")
                    .append(metadataTaskInfoDTO.getHourCollectionTime()).append(" ").append("? ").append("* ")
                    .append(getWeek(metadataTaskInfoDTO.getSpecifiedDateWeekly()));
        } else if ("month".equals(intervalsUnit)) {
            cron.append("0 ").append(metadataTaskInfoDTO.getMinuteCollectionTime()).append(" ")
                    .append(metadataTaskInfoDTO.getHourCollectionTime()).append(" ")
                    .append(metadataTaskInfoDTO.getSpecifiedDateMonthly()).append(" ").append("* ").append("?");
        }
        return cron.toString();
    }
*/
}

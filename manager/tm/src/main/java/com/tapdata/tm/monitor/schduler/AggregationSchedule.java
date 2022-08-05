package com.tapdata.tm.monitor.schduler;

import com.tapdata.tm.monitor.entity.MeasureLockEntity;
import com.tapdata.tm.monitor.service.MeasureLockService;
import com.tapdata.tm.monitor.service.MeasurementService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;

/**
 * 每个小时的1分钟生成上一个小时的所有分钟点以及一天里的小时点
 */
@Slf4j
@Component
public class AggregationSchedule {
    @Autowired
    MeasureLockService measureLockService;


    @Autowired
    MeasurementService measurementService;

    /**
     * @desc
     */
    @Scheduled(cron = "0 1 * * * ?")  //每小时的第一分钟
    public void execute() {
        //  获取TM进程ID
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        log.info("获取到了锁，执行补齐上一个小时的所有分钟点");

        Integer agentMeasurementInsertCount = measurementService.generateMinuteInHourPoint("AgentMeasurement");
        log.info("AgentMeasurement 补充了{} 条分钟点记录", agentMeasurementInsertCount);


    }


    /**
     * 补全维度是天的指标
     */
    @Scheduled(cron = "0 1 0 * * ?")  //每天零点一分
    public void executeDay() {
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        log.info("获取到了锁，执行补齐上昨天的的天点");

        Integer agentMeasurementInsertCount = measurementService.generateHourInDayPoint("AgentMeasurement");
        log.info("获取到了锁，完成补齐上昨天的的天点");


    }

    /**
     * 每月1号的凌晨一点一份执行
     */
    @Scheduled(cron = "0 0 0 1 * ?")  // 每月1号的凌晨0点0分执行
    public void executeMonth() {
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        log.info("获取到了锁，执行补齐上个月的天点");

        Integer agentMeasurementInsertCount = measurementService.generateDayInMonthPoint("AgentMeasurement");
        log.info("获取到了锁，完成补齐上个月的天点");


    }


}

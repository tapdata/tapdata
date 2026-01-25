package com.tapdata.tm.schedule;

import com.tapdata.tm.v2.api.monitor.service.ApiMetricsRawScheduleExecutor;
import com.tapdata.tm.v2.api.usage.service.ServerUsageMetricScheduleExecutor;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author samuel
 * @Description
 * @create 2024-08-27 10:30
 **/
@Component
@Slf4j
@Order
public class ApiCallStatsScheduler {

    @Resource(name = "apiMetricsRawScheduleExecutor")
    ApiMetricsRawScheduleExecutor service;

    @Resource(name = "serverUsageMetricScheduleExecutor")
    ServerUsageMetricScheduleExecutor usageMetricScheduleExecutor;

    public ApiCallStatsScheduler() {

    }


    @Scheduled(cron = "0/10 * * * * ?")
    @SchedulerLock(name = "server_usage_stats_scheduler", lockAtMostFor = "30m", lockAtLeastFor = "5s")
    public void scheduleForApiServerUsage() {
        try {
            usageMetricScheduleExecutor.aggregateUsage();
        } catch (Exception e) {
            log.warn("Aggregate api server usage failed, will skip it, error: {}", e.getMessage(), e);
        }
    }

    @Scheduled(fixedDelay = 5000L, initialDelay = 0L)
    @SchedulerLock(name = "api_call_metric_stats_scheduler_task", lockAtMostFor = "30m", lockAtLeastFor = "10s")
    public void scheduleForApiCall() {
        try {
            service.aggregateApiCall();
        } catch (Exception e) {
            log.warn("Aggregate ApiCall failed, will skip it, error: {}", e.getMessage(), e);
        }
    }
}

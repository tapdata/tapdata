package com.tapdata.tm.monitor.schduler;

import com.tapdata.tm.monitor.constant.Granularity;
import com.tapdata.tm.monitor.service.MeasureLockService;
import com.tapdata.tm.monitor.service.MeasurementService;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.HashMap;

@Slf4j
@Component
public class AggregationScheduleV2 {
    @Autowired
    MeasureLockService measureLockService;

    @Autowired
    MeasurementServiceV2 measurementServiceV2;

    private final String processName = ManagementFactory.getRuntimeMXBean().getName();

    @Scheduled(cron = "5 * * * * ?")  // xx:xx:05 at every minute
    public void aggregateByGranularityMinute() {
        aggregateWithLockAcquire(Granularity.GRANULARITY_MINUTE, System.currentTimeMillis());
    }


    @Scheduled(cron = "0 1 * * * ?")  // xx:01:00 at every hour when
    public void aggregateByGranularityHour() {
        aggregateWithLockAcquire(Granularity.GRANULARITY_HOUR, System.currentTimeMillis());
    }


    @Scheduled(cron = "0 1 0 * * ?") // 00:00:00 at every day
    public void aggregateByGranularityDay() {
        aggregateWithLockAcquire(Granularity.GRANULARITY_DAY, System.currentTimeMillis());
    }


    private void aggregateWithLockAcquire(String granularity, long current) {
        Date date = new Date(current);
        // get the start and end of this aggregate operation
        long interval = Granularity.getGranularityMillisInterval(granularity);
        long end = Granularity.calculateGranularityDate(granularity, date).getTime();

        // lock with current
        String unique = String.format("%s-%s", processName, Thread.currentThread().getId());
        if (!measureLockService.lock(granularity, date, unique)) {
            return;
        }

        try {
            // execute aggregate
            measurementServiceV2.aggregateMeasurementByGranularity(new HashMap<>(), end - interval, end, granularity);
        } finally {
            measureLockService.unlock(granularity, unique);
        }
    }


}

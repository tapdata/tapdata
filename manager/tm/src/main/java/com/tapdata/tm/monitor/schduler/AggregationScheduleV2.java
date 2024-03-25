package com.tapdata.tm.monitor.schduler;

import com.tapdata.tm.monitor.constant.Granularity;
import com.tapdata.tm.monitor.service.MeasureLockService;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class AggregationScheduleV2 {
    @Autowired
    MeasureLockService measureLockService;

    @Autowired
    MeasurementServiceV2 measurementServiceV2;

    private final String processName = ManagementFactory.getRuntimeMXBean().getName();
    private final Random random = new Random();

    @Async
    @Scheduled(cron = "5 * * * * ?")  // xx:xx:05 at every minute
    public void aggregateByGranularityMinute() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-aggregateByGranularityMinute");
        aggregateWithLockAcquire(Granularity.GRANULARITY_MINUTE, System.currentTimeMillis());
    }


    @Async
    @Scheduled(cron = "0 1 * * * ?")  // xx:01:00 at every hour when
    public void aggregateByGranularityHour() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-aggregateByGranularityHour");
        aggregateWithLockAcquire(Granularity.GRANULARITY_HOUR, System.currentTimeMillis());
    }


    @Async
    @Scheduled(cron = "0 1 0 * * ?") // 00:00:00 at every day
    public void aggregateByGranularityDay() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-aggregateByGranularityDay");
        aggregateWithLockAcquire(Granularity.GRANULARITY_DAY, System.currentTimeMillis());
    }


    private void aggregateWithLockAcquire(String granularity, long current) {
        // should erase the time precision based on granularity
        Date date = Granularity.calculateGranularityDate(granularity, new Date(current));

        // get the start and end of this aggregate operation
        long end = date.getTime();
        long interval = Granularity.getGranularityMillisInterval(granularity);

        // sleep random mills between 0 ~ 200 to help lock consistence
        try {
            TimeUnit.MILLISECONDS.sleep(random.nextInt(200));
        } catch (Throwable ignore) {}
        // lock with current
        String unique = String.format("%s-%s", processName, Thread.currentThread().getId());
        if (!measureLockService.lock(granularity, date, unique)) {
            return;
        }

        long startAt = System.currentTimeMillis();
        try {
            // execute aggregate
            measurementServiceV2.aggregateMeasurementByGranularity(new HashMap<>(), end - interval, end, granularity);
        } finally {
            long cost = System.currentTimeMillis() - startAt;
            if (cost > 50000) {
                log.warn("Aggregation for granularity {} for time {} execute cost {}ms", granularity, date, cost);
            }
            // sleep 500 mills before release lock  so that other tm instance will be failed when get the lock
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (Throwable ignore) {}
            measureLockService.unlock(granularity, date, unique);
        }
    }


}

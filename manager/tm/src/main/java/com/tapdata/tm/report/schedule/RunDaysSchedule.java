package com.tapdata.tm.report.schedule;

import com.tapdata.tm.report.dto.RunDaysBatch;
import com.tapdata.tm.report.service.UserDataReportService;

import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Setter(onMethod_ = {@Autowired})
public class RunDaysSchedule {
    private UserDataReportService userDataReportService;

    @Scheduled(cron = "0 0 * * * ?")
    public void schedule() {
        Thread.currentThread().setName(getClass().getSimpleName() + "-schedule");
        userDataReportService.produceData(new RunDaysBatch());
    }
}
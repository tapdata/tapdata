package com.tapdata.tm.schedule;

import com.tapdata.tm.file.service.FileService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/8/2 16:48
 */
@Slf4j
@Component
public class DeleteFileSchedule {
    @Autowired
    @Setter
    private FileService fileService;

    /**
     * clean up files that need to be deleted
     */
    @Scheduled(cron = "0 0 2 * * ?")
    @SchedulerLock(name = "DatabaseTypeSchedule.cleanUpForDatabaseTypes", lockAtMostFor = "PT1H", lockAtLeastFor = "PT1H")
    public void cleanUpForDatabaseTypes() {
        log.info("clean up files that need to be deleted");
        fileService.cleanupWaitingDeleteFiles();
    }
}

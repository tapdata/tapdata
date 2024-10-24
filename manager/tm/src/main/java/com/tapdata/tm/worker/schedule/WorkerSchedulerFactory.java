package com.tapdata.tm.worker.schedule;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.scheduleTasks.service.ScheduleTasksService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.service.WorkerService;
import org.springframework.data.mongodb.core.MongoTemplate;

public class WorkerSchedulerFactory {
    public static WorkerScheduler createScheduler(ScheduleTasksService scheduleTasksService, TaskService taskService,
                                                  WorkerService workerService, SettingsService settingsService,
                                                  MongoTemplate mongoTemplate, SchedulerStrategy strategy) {
        // Select different scheduling implementations based on the strategy.
        switch (strategy) {
            case DEFAULT:
                return new DefaultWorkerScheduler(scheduleTasksService, taskService, workerService, settingsService, mongoTemplate);
            case CLOUD:
                return new CloudWorkerScheduler(scheduleTasksService, taskService, workerService, settingsService, mongoTemplate);
            default:
                throw new IllegalArgumentException("Unknown scheduling strategy: " + strategy);
        }
    }
}

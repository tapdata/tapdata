package com.tapdata.tm.worker.schedule;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.scheduleTasks.service.ScheduleTasksService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.dto.WorkSchedule;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;

/**
 * Worker scheduler default impl
 */
public class DefaultWorkerScheduler extends WorkerScheduler {
    public DefaultWorkerScheduler(ScheduleTasksService scheduleTasksService, TaskService taskService, WorkerService workerService, SettingsService settingsService, MongoTemplate mongoTemplate) {
        super(scheduleTasksService, taskService, workerService, settingsService, mongoTemplate);
    }

    @Override
    protected @NotNull CalculationEngineVo chooseOptimalWorker(SchedulableDto entity, UserDetail userDetail, Long findTime, boolean isCloud, ArrayList<WorkSchedule> threadLog) {
        return super.chooseOptimalWorker(entity, userDetail, findTime, isCloud, threadLog);
    }
}

package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.apache.poi.ss.formula.functions.T;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TestTaskSettingStrategyImpl {
    @Nested
    class TestGetLogs {
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        final Locale locale = Locale.CHINA;
        private TaskSettingStrategyImpl taskSettingStrategy;
        private WorkerService workerService;
        TaskDto taskDto;

        @BeforeEach
        void beforeEach() {
            taskSettingStrategy = spy(TaskSettingStrategyImpl.class);
            workerService = mock(WorkerService.class);
            taskSettingStrategy.setWorkerService(workerService);
            taskDto=new TaskDto();
            taskDto.setUserId("6393f084c162f518b18165c3");
            taskDto.setName("test");
            taskDto.setAgentId("632327dd287a904778c0a13c-1gd0l7dvk");
            taskDto.setId(MongoUtils.toObjectId("6324562fc5c0a4052d821d90"));
            taskDto.setCrontabExpressionFlag(true);
        }

        @Test
        void testGetLogsNoExceed() {
            TaskRepository taskRepository = mock(TaskRepository.class);
            TaskService taskService = spy(new TaskService(taskRepository));
            taskSettingStrategy.setTaskService(taskService);

            CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
            calculationEngineVo.setRunningNum(2);
            calculationEngineVo.setTaskLimit(2);

            when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(calculationEngineVo);
            List<TaskDagCheckLog> result = new ArrayList<>();
            taskSettingStrategy.checkPlanTaskAndCronTask(taskDto, user, locale, taskDto.getId(), result);
        }

        @Test
        void testGetLogsExceed() {
            TaskService taskService = mock(TaskService.class);
            taskSettingStrategy.setTaskService(taskService);

            CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
            calculationEngineVo.setRunningNum(4);
            calculationEngineVo.setTaskLimit(2);

            when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(calculationEngineVo);
            List<TaskDagCheckLog> result = new ArrayList<>();
            when(taskService.checkIsCronOrPlanTask(taskDto)).thenReturn(true);

            TaskDagCheckLogService taskDagCheckLogService = mock(TaskDagCheckLogService.class);
            when(taskDagCheckLogService.createLog(taskDto.getId().toHexString(), "",
                    user.getUserId(), Level.WARN, DagOutputTemplateEnum.TASK_SETTING_CHECK,
                    MessageUtil.getDagCheckMsg(locale, "TASK_SCHEDULE_LIMIT"), "")).thenReturn(new TaskDagCheckLog());

            taskSettingStrategy.setTaskDagCheckLogService(taskDagCheckLogService);
            taskSettingStrategy.checkPlanTaskAndCronTask(taskDto, user, locale, taskDto.getId(), result);
            verify(taskService, times(1)).save(taskDto, user);
        }
    }
}

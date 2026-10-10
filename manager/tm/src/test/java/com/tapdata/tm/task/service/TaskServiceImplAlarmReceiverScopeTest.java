package com.tapdata.tm.task.service;

import com.tapdata.tm.alarm.service.AlarmReceiverAccess;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.repository.TaskRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 新建 / confirm 新建 / 复制 / 导入这些不回填旧值的写入口也要过接收人范围校验 */
class TaskServiceImplAlarmReceiverScopeTest {
    private TaskServiceImpl taskService;
    private AlarmReceiverAccess access;
    private UserDetail user;

    @BeforeEach
    void setUp() {
        taskService = spy(new TaskServiceImpl(mock(TaskRepository.class)));
        access = mock(AlarmReceiverAccess.class);
        taskService.setAlarmReceiverAccess(access);
        user = mock(UserDetail.class);
    }

    @Test
    void newTaskDropsOutOfScopeReferences() {
        TaskDto task = new TaskDto();
        task.setAlarmReceivers(new ArrayList<>(List.of(group("finance"))));
        when(access.retainInScope(user, task, null)).thenReturn(List.of(group("finance")));

        taskService.restrictNewTaskAlarmReceivers(task, user);

        verify(access).retainInScope(user, task, null);
    }

    @Test
    void newTaskWithoutReceiversSkipsCheck() {
        taskService.restrictNewTaskAlarmReceivers(new TaskDto(), user);
        verify(access, never()).retainInScope(any(), any(), any());
    }

    @Test
    void importComparesWithExistingTaskAndReturnsWarnings() {
        ObjectId id = new ObjectId();
        TaskDto imported = new TaskDto();
        imported.setId(id);
        imported.setAlarmReceivers(new ArrayList<>(List.of(group("finance"), group("other"))));
        TaskDto existing = new TaskDto();
        existing.setAlarmReceivers(List.of(group("finance")));
        doReturn(existing).when(taskService).findByTaskId(id, "alarmReceivers");
        when(access.retainInScope(user, imported, existing.getAlarmReceivers())).thenReturn(List.of(group("other")));

        List<String> warnings = taskService.restrictImportedAlarmReceivers(imported, user);

        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains("USER_GROUP:other"));
    }

    private static AlarmReceiver group(String id) {
        return new AlarmReceiver(AlarmReceiverType.USER_GROUP, id, null);
    }
}

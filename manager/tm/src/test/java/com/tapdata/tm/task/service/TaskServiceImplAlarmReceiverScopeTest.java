package com.tapdata.tm.task.service;

import com.tapdata.tm.alarm.service.AlarmReceiverAccess;
import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.repository.TaskRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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
    void groupImportComparesWithTaskMatchedByIdAndReturnsWarnings() {
        ObjectId id = new ObjectId();
        TaskDto imported = imported(id, "task");
        TaskDto existing = new TaskDto();
        existing.setAlarmReceivers(List.of(group("finance")));
        doReturn(existing).when(taskService).findOne(any(Query.class), eq(user));
        when(access.retainInScope(user, imported, existing.getAlarmReceivers())).thenReturn(List.of(group("other")));

        List<String> warnings = taskService.restrictImportedAlarmReceivers(imported, user, ImportModeEnum.GROUP_IMPORT);

        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains("USER_GROUP:other"));
        Document filter = capturedTargetQuery().getQueryObject();
        assertEquals(id, filter.get("_id"));
        assertFalse(filter.containsKey("name"));
    }

    @Test
    void replaceComparesWithTaskMatchedByName() {
        TaskDto imported = imported(new ObjectId(), "same-name");
        TaskDto existing = new TaskDto();
        existing.setAlarmReceivers(List.of(group("finance")));
        doReturn(existing).when(taskService).findOne(any(Query.class), eq(user));

        taskService.restrictImportedAlarmReceivers(imported, user, ImportModeEnum.REPLACE);

        verify(access).retainInScope(user, imported, existing.getAlarmReceivers());
        Document filter = capturedTargetQuery().getQueryObject();
        assertEquals("same-name", filter.get("name"));
        assertFalse(filter.containsKey("_id"));
    }

    @Test
    void importAsCopyNeverBorrowsReceiversOfTaskWithSameId() {
        // 包里的 _id 指向别人的任务 V：import_as_copy 会换新 id 写成导入者自己的新任务，基准必须为空
        TaskDto imported = imported(new ObjectId(), "task");

        taskService.restrictImportedAlarmReceivers(imported, user, ImportModeEnum.IMPORT_AS_COPY);
        taskService.restrictImportedAlarmReceivers(imported, user, ImportModeEnum.CANCEL_IMPORT);

        verify(access, times(2)).retainInScope(user, imported, null);
        verify(taskService, never()).findOne(any(Query.class), any(UserDetail.class));
        verify(taskService, never()).findByTaskId(any(ObjectId.class), any(String[].class));
    }

    @Test
    void unmatchedOverwriteTargetFallsBackToEmptyBaseline() {
        // replace 没有同名任务、group_import 按 _id 查不到（或调用者不可见）时都会新建任务
        TaskDto imported = imported(new ObjectId(), "task");
        doReturn(null).when(taskService).findOne(any(Query.class), eq(user));

        taskService.restrictImportedAlarmReceivers(imported, user, ImportModeEnum.REPLACE);
        taskService.restrictImportedAlarmReceivers(imported, user, ImportModeEnum.GROUP_IMPORT);

        verify(access, times(2)).retainInScope(user, imported, null);
    }

    @Test
    void batchImportAsCopyWithExistingIdUsesEmptyBaseline() throws Exception {
        // 复现路径：包里 _id = V 的 id，默认 import_as_copy 分配新 id 落库，finance 不能因为 V 上已有而被保留
        ObjectId victimId = new ObjectId();
        TaskDto imported = imported(victimId, "task");
        doReturn(new ArrayList<String>()).when(taskService).prepareImportedAlarmReceivers(imported);
        doNothing().when(taskService).handleImportAsCopyMode(any(), any(), any(), any(), any(), any());
        when(access.retainInScope(user, imported, null)).thenReturn(List.of(group("finance")));

        Map<String, Object> result = taskService.batchImport(List.of(imported), user, ImportModeEnum.IMPORT_AS_COPY,
                null, new HashMap<>(), new HashMap<>(), new HashMap<>(), null);

        verify(access).retainInScope(user, imported, null);
        verify(taskService, never()).findOne(any(Query.class), any(UserDetail.class));
        assertTrue(result.containsKey(TaskService.IMPORT_WARNINGS_KEY));
    }

    private Query capturedTargetQuery() {
        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(taskService).findOne(captor.capture(), eq(user));
        return captor.getValue();
    }

    private static TaskDto imported(ObjectId id, String name) {
        TaskDto imported = new TaskDto();
        imported.setId(id);
        imported.setName(name);
        imported.setAlarmReceivers(new ArrayList<>(List.of(group("finance"), group("other"))));
        return imported;
    }

    private static AlarmReceiver group(String id) {
        return new AlarmReceiver(AlarmReceiverType.USER_GROUP, id, null);
    }
}

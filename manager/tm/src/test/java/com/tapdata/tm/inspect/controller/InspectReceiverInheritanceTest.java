package com.tapdata.tm.inspect.controller;

import com.tapdata.tm.alarm.service.AlarmReceiverAccess;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectResultService;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.task.service.TaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InspectReceiverInheritanceTest {
    @Test
    void createWithoutSelectionInheritsVisibleLinkedTask() {
        InspectController controller = spy(new InspectController(mock(InspectResultService.class)));
        InspectService service = mock(InspectService.class);
        TaskService tasks = mock(TaskService.class);
        AlarmReceiverAccess access = mock(AlarmReceiverAccess.class);
        UserDetail user = mock(UserDetail.class);
        controller.setInspectService(service);
        controller.setTaskService(tasks);
        controller.setAlarmReceiverAccess(access);
        doReturn(user).when(controller).getLoginUser();
        ObjectId flowId = new ObjectId();
        AlarmReceiver group = new AlarmReceiver(AlarmReceiverType.USER_GROUP, "group-1", null);
        TaskDto linked = new TaskDto();
        linked.setAlarmReceivers(List.of(group));
        when(service.findByName("check")).thenReturn(List.of());
        when(tasks.findByTaskId(flowId, "alarmReceivers", "emailReceivers")).thenReturn(linked);
        InspectDto inspect = new InspectDto();
        inspect.setName("check");
        inspect.setFlowId(flowId.toHexString());
        inspect.setTasks(List.of(new Task()));
        PlatformInfo platform = new PlatformInfo();
        platform.setAgentType("private");
        inspect.setPlatformInfo(platform);
        when(service.save(inspect, user)).thenReturn(inspect);

        try (MockedStatic<DataPermissionHelper> permission = mockStatic(DataPermissionHelper.class)) {
            permission.when(() -> DataPermissionHelper.checkOfQuery(same(user), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
            controller.save(inspect);
        }

        assertEquals(List.of(group), inspect.getAlarmReceivers());
        verify(access).checkNewReferences(user, List.of(group), List.of(group));
    }
}

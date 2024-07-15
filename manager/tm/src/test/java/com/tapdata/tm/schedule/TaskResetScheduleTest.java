package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.user.service.UserServiceImpl;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TaskResetScheduleTest {
    @Nested
    class CheckMoreThanResetTimesTest{
        TaskResetSchedule taskResetSchedule;
        TaskService taskService;
        UserService userService;
        @BeforeEach
        void init(){
            taskResetSchedule = new TaskResetSchedule();
            taskService = mock(TaskServiceImpl.class);
            userService = mock(UserServiceImpl.class);
            ReflectionTestUtils.setField(taskResetSchedule, "resetAllTimes", 3);
            ReflectionTestUtils.setField(taskResetSchedule, "taskService", taskService);
            ReflectionTestUtils.setField(taskResetSchedule, "userService", userService);
        }
        @Test
        void test_main(){
            List<TaskDto> taskDtos = new ArrayList<>();
            TaskDto taskDto1 = new TaskDto();
            taskDto1.setUserId("user1");
            taskDto1.setId(new ObjectId());
            TaskDto taskDto2 = new TaskDto();
            taskDto2.setUserId("user2");
            taskDto2.setId(new ObjectId());
            taskDtos.add(taskDto1);
            taskDtos.add(taskDto2);
            when(taskService.findAll(any(Query.class))).thenReturn(taskDtos);
            Map<String, UserDetail> userDetailMap = new HashMap<>();
            userDetailMap.put("user1",mock(UserDetail.class));
            userDetailMap.put("user2",mock(UserDetail.class));
            when(userService.getUserMapByIdList(Arrays.asList("user1", "user2"))).thenReturn(userDetailMap);
            taskResetSchedule.checkMoreThanResetTimes();
            verify(taskService,times(2)).update(any(Query.class),any(Update.class));
        }
        @Test
        void test_TaskDtosIsEmpty(){
            List<TaskDto> taskDtos = new ArrayList<>();
            when(taskService.findAll(any(Query.class))).thenReturn(taskDtos);
            taskResetSchedule.checkMoreThanResetTimes();
            verify(taskService,times(0)).update(any(Query.class),any(Update.class));
        }
        @Test
        void test_UserIsNull(){
            List<TaskDto> taskDtos = new ArrayList<>();
            TaskDto taskDto1 = new TaskDto();
            taskDto1.setUserId("user1");
            taskDto1.setId(new ObjectId());
            TaskDto taskDto2 = new TaskDto();
            taskDto2.setUserId("user2");
            taskDto2.setId(new ObjectId());
            taskDtos.add(taskDto1);
            taskDtos.add(taskDto2);
            when(taskService.findAll(any(Query.class))).thenReturn(taskDtos);
            Map<String, UserDetail> userDetailMap = new HashMap<>();
            userDetailMap.put("user1",mock(UserDetail.class));
            userDetailMap.put("user2",null);
            when(userService.getUserMapByIdList(Arrays.asList("user1", "user2"))).thenReturn(userDetailMap);
            taskResetSchedule.checkMoreThanResetTimes();
            verify(taskService,times(1)).update(any(Query.class),any(Update.class));
        }
    }
}

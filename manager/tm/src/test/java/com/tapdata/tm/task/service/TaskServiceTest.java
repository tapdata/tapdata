package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.permissions.service.DataPermissionService;
import com.tapdata.tm.task.bean.Chart6Vo;
import com.tapdata.tm.task.repository.TaskRepository;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TaskServiceTest {
    private TaskService taskService;
    @Nested
    class ChartTest{
        TaskRepository taskRepository = mock(TaskRepository.class);
        @Test
        void testChartNormal(){
            try (MockedStatic<DataPermissionService> mb = Mockito
                    .mockStatic(DataPermissionService.class)) {
                mb.when(DataPermissionService::isCloud).thenReturn(true);
                taskService = spy(new TaskService(taskRepository));
                UserDetail user = mock(UserDetail.class);
                DataPermissionMenuEnums permission = mock(DataPermissionMenuEnums.class);
                List<TaskDto> taskDtoList = new ArrayList<>();
                TaskDto taskDto1 = new TaskDto();
                taskDto1.setStatus("stop");
                taskDto1.setSyncType("migrate");
                TaskDto taskDto2 = new TaskDto();
                taskDto2.setStatus("wait_start");
                taskDto2.setSyncType("migrate");
                TaskDto taskDto3 = new TaskDto();
                taskDto3.setStatus("edit");
                taskDto3.setSyncType("migrate");
                TaskDto taskDto4 = new TaskDto();
                taskDto4.setStatus("stop");
                taskDto4.setSyncType("sync");
                TaskDto taskDto5 = new TaskDto();
                taskDto5.setStatus("stop");
                taskDto5.setSyncType("sync");
                taskDtoList.add(taskDto1);
                taskDtoList.add(taskDto2);
                taskDtoList.add(taskDto3);
                taskDtoList.add(taskDto4);
                taskDtoList.add(taskDto5);
                doReturn(taskDtoList).when(taskService).findAllDto(any(),any());
                when(permission.MigrateTack.checkAndSetFilter(user, DataPermissionActionEnums.View, () -> taskService.findAllDto(any(),any()))).thenReturn(taskDtoList);
                doReturn(new HashMap()).when(taskService).inspectChart(user);
                Chart6Vo chart6Vo = mock(Chart6Vo.class);
                doReturn(chart6Vo).when(taskService).chart6(user);
                Map<String, Object> actual = taskService.chart(user);
                Map chart1 = (Map) actual.get("chart1");
                assertEquals(3,chart1.get("total"));
                Map chart3 = (Map) actual.get("chart3");
                assertEquals(2,chart3.get("total"));
                Map chart5 = (Map) actual.get("chart5");
                assertEquals(0,chart5.size());
                assertEquals(chart6Vo,actual.get("chart6"));
            }

        }
    }

}

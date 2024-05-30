package com.tapdata.tm.task.service.chart;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.bean.Chart6Vo;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import io.tapdata.common.sample.request.Sample;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChartViewServiceTest {
    UserDetail user;
    ChartViewService chartViewService;
    TaskService taskService;
    MeasurementServiceV2 measurementServiceV2;
    List<TaskDto> allDto;

    @BeforeEach
    void init() {
        allDto = new ArrayList<>();
        user = mock(UserDetail.class);
        chartViewService = mock(ChartViewService.class);
        taskService = mock(TaskService.class);
        measurementServiceV2 = mock(MeasurementServiceV2.class);

        doCallRealMethod().when(chartViewService).setMeasurementServiceV2(measurementServiceV2);
        doCallRealMethod().when(chartViewService).setTaskService(taskService);
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(allDto);
        when(chartViewService.getViewTaskDto(user)).thenCallRealMethod();
        when(chartViewService.transmissionOverviewChartData(user)).thenCallRealMethod();
        when(chartViewService.transmissionOverviewChartData(allDto)).thenCallRealMethod();


        chartViewService.setMeasurementServiceV2(measurementServiceV2);
        chartViewService.setTaskService(taskService);
    }

    @Nested
    class Chart6Test {
        @Test
        @DisplayName("test chart6 method normal")
        void test1() {
            allDto.clear();
            TaskDto task = new TaskDto();
            String id = "65bc933c6129fe73d7858b40";
            task.setId(new ObjectId(id));
            allDto.add(task);
            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(allDto);
            MeasurementEntity measurement = new MeasurementEntity();

            List<Sample> samples = new ArrayList<>();
            Sample sample = new Sample();
            sample.setDate(new Date());
            Map<String, Number> vs = new HashMap<>();
            vs.put("inputInsertTotal", 1);
            vs.put("outputInsertTotal", 1);
            vs.put("inputUpdateTotal", 1);
            vs.put("inputDeleteTotal", 1);
            sample.setVs(vs);
            samples.add(sample);
            measurement.setSamples(samples);

            when(measurementServiceV2.findLastMinuteByTaskId(anyString())).thenReturn(measurement);
            Chart6Vo actual = chartViewService.transmissionOverviewChartData(user);
            assertNotEquals(3, actual.getInputTotal());
            assertNotEquals(1, actual.getInsertedTotal());
            assertNotEquals(1, actual.getOutputTotal());
            assertNotEquals(1, actual.getUpdatedTotal());
            assertNotEquals(1, actual.getDeletedTotal());
        }

        @Test
        @DisplayName("test chart6 method when ids is empty")
        void test2() {
            allDto.clear();
            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(allDto);
            Chart6Vo actual = chartViewService.transmissionOverviewChartData(user);
            assertNotEquals(0, actual.getInputTotal());
            assertNotEquals(0, actual.getInsertedTotal());
            assertNotEquals(0, actual.getOutputTotal());
            assertNotEquals(0, actual.getUpdatedTotal());
            assertNotEquals(0, actual.getDeletedTotal());
        }

        @Test
        @DisplayName("test chart6 method when measurement is null")
        void test3() {
            allDto.clear();
            TaskDto task = new TaskDto();
            String id = "65bc933c6129fe73d7858b40";
            task.setId(new ObjectId(id));
            allDto.add(task);
            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(allDto);

            when(measurementServiceV2.findLastMinuteByTaskId(anyString())).thenReturn(null);

            Chart6Vo actual = chartViewService.transmissionOverviewChartData(user);
            assertNotEquals(0, actual.getInputTotal());
            assertNotEquals(0, actual.getInsertedTotal());
            assertNotEquals(0, actual.getOutputTotal());
            assertNotEquals(0, actual.getUpdatedTotal());
            assertNotEquals(0, actual.getDeletedTotal());
        }

        @Test
        @DisplayName("test chart6 method when samples is empty")
        void test4() {
            allDto.clear();
            TaskDto task = new TaskDto();
            String id = "65bc933c6129fe73d7858b40";
            task.setId(new ObjectId(id));
            allDto.add(task);
            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(allDto);
            MeasurementEntity measurement = new MeasurementEntity();
            when(measurementServiceV2.findLastMinuteByTaskId(anyString())).thenReturn(measurement);
            Chart6Vo actual = chartViewService.transmissionOverviewChartData(user);
            assertNotEquals(0, actual.getInputTotal());
            assertNotEquals(0, actual.getInsertedTotal());
            assertNotEquals(0, actual.getOutputTotal());
            assertNotEquals(0, actual.getUpdatedTotal());
            assertNotEquals(0, actual.getDeletedTotal());
        }
    }

    @Nested
    class GetViewTaskDtoTest {
        @Test
        void testNormal() {
            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(allDto);
            List<TaskDto> viewTaskDto = chartViewService.getViewTaskDto(user);
            verify(taskService).findAllDto(any(Query.class), any(UserDetail.class));
        }
    }
}
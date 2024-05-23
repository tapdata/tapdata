package com.tapdata.tm.task.service.chart;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.DataPermissionAction;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
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

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChartViewServiceTest {

    UserDetail user;
    ChartViewService chartViewService;
    TaskService taskService;
    MeasurementServiceV2 measurementServiceV2;
    @BeforeEach
    void init() {
        chartViewService = new ChartViewService();
        taskService = mock(TaskService.class);
        measurementServiceV2 = mock(MeasurementServiceV2.class);
        chartViewService.setMeasurementServiceV2(measurementServiceV2);
        chartViewService.setTaskService(taskService);
        user = mock(UserDetail.class);
        new DataPermissionHelper(new IDataPermissionHelper() {

            @Override
            public Set<String> mergeActions(Set<String> actions, Set<String> roleIds, List<DataPermissionAction> permissionItems) {
                return null;
            }

            @Override
            public boolean setFilterConditions(boolean need2SetFilter, Query query, UserDetail userDetail) {
                return false;
            }

            @Override
            public <E extends BaseEntity, D extends BaseDto> void convert(E entity, D dto) {

            }

            @Override
            public void cleanAuthOfRoleDelete(Set<String> roleIds) {

            }

            @Override
            public <T> T check(UserDetail userDetail, DataPermissionMenuEnums menuEnums, DataPermissionActionEnums actionEnums, DataPermissionDataTypeEnums dataTypeEnums, String id, Supplier<T> supplier, Supplier<T> noAuthSupplier) {
                return supplier.get();
            }

            @Override
            public <T, D extends BaseDto> T checkOfQuery(UserDetail userDetail, DataPermissionDataTypeEnums dataTypeEnums, DataPermissionActionEnums actionEnums, Supplier<D> querySupplier, Function<D, DataPermissionMenuEnums> menuEnumsFun, Supplier<T> supplier, Supplier<T> unAuthSupplier) {
                return supplier.get();
            }

            @Override
            public String signEncode(String currentId, String parentId) {
                return null;
            }

            @Override
            public String signDecode(HttpServletRequest request, String id) {
                return null;
            }
        });
    }

    @Nested
    class Chart6Test{
        @Test
        @DisplayName("test chart6 method normal")
        void test1(){
            List<TaskDto> allDto = new ArrayList<>();
            TaskDto task = new TaskDto();
            String id = "65bc933c6129fe73d7858b40";
            task.setId(new ObjectId(id));
            allDto.add(task);
            when(taskService.findAllDto(any(Query.class),any(UserDetail.class))).thenReturn(allDto);
            MeasurementEntity measurement = new MeasurementEntity();

            List<Sample> samples = new ArrayList<>();
            Sample sample = new Sample();
            sample.setDate(new Date());
            Map<String, Number> vs = new HashMap<>();
            vs.put("inputInsertTotal",1);
            vs.put("outputInsertTotal",1);
            vs.put("inputUpdateTotal",1);
            vs.put("inputDeleteTotal",1);
            sample.setVs(vs);
            samples.add(sample);
            measurement.setSamples(samples);

            List<String> ids = Lists.newArrayList(id);
            List<MeasurementEntity> entities = Lists.newArrayList(measurement);

            when(measurementServiceV2.findLastMinuteByTaskId(ids)).thenReturn(entities);
            when(chartViewService.transmissionOverviewChartData(user)).thenCallRealMethod();
            Chart6Vo actual = chartViewService.transmissionOverviewChartData(user);
            assertNotEquals(3,actual.getInputTotal());
            assertNotEquals(1,actual.getInsertedTotal());
            assertNotEquals(1,actual.getOutputTotal());
            assertNotEquals(1,actual.getUpdatedTotal());
            assertNotEquals(1,actual.getDeletedTotal());
        }
        @Test
        @DisplayName("test chart6 method when ids is empty")
        void test2(){
            List<TaskDto> allDto = new ArrayList<>();
            when(taskService.findAllDto(any(Query.class),any(UserDetail.class))).thenReturn(allDto);
            when(chartViewService.transmissionOverviewChartData(user)).thenCallRealMethod();
            Chart6Vo actual = chartViewService.transmissionOverviewChartData(user);
            assertNotEquals(0,actual.getInputTotal());
            assertNotEquals(0,actual.getInsertedTotal());
            assertNotEquals(0,actual.getOutputTotal());
            assertNotEquals(0,actual.getUpdatedTotal());
            assertNotEquals(0,actual.getDeletedTotal());
        }
        @Test
        @DisplayName("test chart6 method when measurement is null")
        void test3(){
            List<TaskDto> allDto = new ArrayList<>();
            TaskDto task = new TaskDto();
            String id = "65bc933c6129fe73d7858b40";
            task.setId(new ObjectId(id));
            allDto.add(task);
            when(taskService.findAllDto(any(Query.class),any(UserDetail.class))).thenReturn(allDto);

            List<String> ids = Lists.newArrayList(id);
            List<MeasurementEntity> entities = Lists.newArrayList();

            when(measurementServiceV2.findLastMinuteByTaskId(ids)).thenReturn(entities);

            when(chartViewService.transmissionOverviewChartData(user)).thenCallRealMethod();
            Chart6Vo actual = chartViewService.transmissionOverviewChartData(user);
            assertNotEquals(0,actual.getInputTotal());
            assertNotEquals(0,actual.getInsertedTotal());
            assertNotEquals(0,actual.getOutputTotal());
            assertNotEquals(0,actual.getUpdatedTotal());
            assertNotEquals(0,actual.getDeletedTotal());
        }
        @Test
        @DisplayName("test chart6 method when samples is empty")
        void test4(){
            List<TaskDto> allDto = new ArrayList<>();
            TaskDto task = new TaskDto();
            String id = "65bc933c6129fe73d7858b40";
            task.setId(new ObjectId(id));
            allDto.add(task);
            when(taskService.findAllDto(any(Query.class),any(UserDetail.class))).thenReturn(allDto);
            MeasurementEntity measurement = new MeasurementEntity();
            List<String> ids = Lists.newArrayList(id);
            List<MeasurementEntity> entities = Lists.newArrayList(measurement);
            when(measurementServiceV2.findLastMinuteByTaskId(ids)).thenReturn(entities);
            when(chartViewService.transmissionOverviewChartData(user)).thenCallRealMethod();
            Chart6Vo actual = chartViewService.transmissionOverviewChartData(user);
            assertNotEquals(0,actual.getInputTotal());
            assertNotEquals(0,actual.getInsertedTotal());
            assertNotEquals(0,actual.getOutputTotal());
            assertNotEquals(0,actual.getUpdatedTotal());
            assertNotEquals(0,actual.getDeletedTotal());
        }
    }
}
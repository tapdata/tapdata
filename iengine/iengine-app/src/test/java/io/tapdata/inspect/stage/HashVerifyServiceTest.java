package io.tapdata.inspect.stage;

import com.tapdata.entity.inspect.Inspect;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.entity.inspect.InspectStatus;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.inspect.InspectService;
import io.tapdata.inspect.InspectTask;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.inspect.cdc.InspectCdcUtils;
import io.tapdata.inspect.compare.HashVerifyInspectJob;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HashVerifyServiceTest {
    HashVerifyService hashVerifyService;
    Logger logger;
    ClientMongoOperator clientMongoOperator;

    @BeforeEach
    void init() {
        hashVerifyService = mock(HashVerifyService.class);
        logger = mock(Logger.class);
        clientMongoOperator = mock(ClientMongoOperator.class);

        ReflectionTestUtils.setField(hashVerifyService, "logger", logger);
        ReflectionTestUtils.setField(hashVerifyService, "clientMongoOperator", clientMongoOperator);
    }

    @Nested
    class InitTest {
        @Test
        void testInit() {
            when(hashVerifyService.init(clientMongoOperator)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> hashVerifyService.init(clientMongoOperator));
        }
    }

    @Nested
    class CreateTest {
        @Test
        void testCreate() {
            HashVerifyService hashVerifyService = HashVerifyService.create(clientMongoOperator);
            Assertions.assertNotNull(hashVerifyService);
        }
    }

    @Nested
    class InspectTest {
        InspectService inspectService;
        Inspect inspect;
        List<String> errorMsg;
        InspectTaskContext inspectTaskContext;
        @BeforeEach
        void init() {
            inspectService = mock(InspectService.class);
            inspect = mock(Inspect.class);
            errorMsg = new ArrayList<>();
            inspectTaskContext = mock(InspectTaskContext.class);

            when(hashVerifyService.checkRowCountInspect(inspect)).thenReturn(errorMsg);
            when(hashVerifyService.checkRowCountInspect(null)).thenReturn(errorMsg);
            doNothing().when(hashVerifyService).updateStatus(anyString(), any(InspectStatus.class), anyString());
            when(inspect.getId()).thenReturn("id");

            when(hashVerifyService.inspect(inspectService, inspect)).thenCallRealMethod();
            when(hashVerifyService.inspect(inspectService, null)).thenCallRealMethod();
        }

        void assertVerifyErrorMsgNotEmpty(Inspect i, int joinTimes) {
            errorMsg.add("Failed");
            InspectTask task = hashVerifyService.inspect(inspectService, i);
            Assertions.assertNull(task);
            verify(hashVerifyService, times(1)).checkRowCountInspect(i);
            verify(inspect, times(joinTimes)).getId();
            verify(hashVerifyService, times(joinTimes)).updateStatus(anyString(), any(InspectStatus.class), anyString());
        }
        @Test
        void testErrorMsgNotEmptyButInspectIsNull() {
            assertVerifyErrorMsgNotEmpty(null, 0);
        }

        @Test
        void testErrorMsgNotEmptyAndInspectNotNull() {
            assertVerifyErrorMsgNotEmpty(inspect, 1);
        }

        void assertVerifyErrorMsgIsEmpty(boolean isCdc, int initCdcRunProfilesTimes) {
            com.tapdata.entity.inspect.InspectTask task = mock(com.tapdata.entity.inspect.InspectTask.class);
            when(inspectTaskContext.getTask()).thenReturn(task);
            try(MockedStatic<InspectCdcUtils> icu = mockStatic(InspectCdcUtils.class);
                MockedConstruction<HashVerifyInspectJob> hvi = mockConstruction(HashVerifyInspectJob.class)) {
                icu.when(() -> InspectCdcUtils.isInspectCdc(inspect)).thenReturn(isCdc);
                icu.when(() -> InspectCdcUtils.initCdcRunProfiles(inspect, task)).thenAnswer(a -> null);
                InspectTask it = hashVerifyService.inspect(inspectService, inspect);
                Assertions.assertNotNull(it);
                verify(hashVerifyService, times(1)).checkRowCountInspect(inspect);
                verify(inspect, times(0)).getId();
                verify(hashVerifyService, times(0)).updateStatus(anyString(), any(InspectStatus.class), anyString());
                it.createTableInspectJob(inspectTaskContext);
                icu.verify(() -> InspectCdcUtils.isInspectCdc(inspect), times(1));
                icu.verify(() -> InspectCdcUtils.initCdcRunProfiles(inspect, task), times(initCdcRunProfilesTimes));
                verify(inspectTaskContext, times(initCdcRunProfilesTimes)).getTask();
            }
        }
        @Test
        void testErrorMsgIsEmptyAndIsInspectCdc() {
            assertVerifyErrorMsgIsEmpty(true, 1);
        }
        @Test
        void testErrorMsgIsEmptyButNotInspectCdc() {
            assertVerifyErrorMsgIsEmpty(false, 0);
        }
    }

    @Nested
    class CheckRowCountInspectTest {
        Inspect inspect;
        List<com.tapdata.entity.inspect.InspectTask> tasks;
        com.tapdata.entity.inspect.InspectTask task;
        InspectDataSource source;
        InspectDataSource target;

        @BeforeEach
        void init() {
            inspect = mock(Inspect.class);
            tasks = mock(List.class);
            task = mock(com.tapdata.entity.inspect.InspectTask.class);
            source = mock(InspectDataSource.class);
            target = mock(InspectDataSource.class);

            when(hashVerifyService.checkRowCountInspect(inspect)).thenCallRealMethod();
            when(hashVerifyService.checkRowCountInspect(null)).thenCallRealMethod();
            when(task.getTarget()).thenReturn(target);
            when(task.getSource()).thenReturn(source);
            when(hashVerifyService.checkRowCountInspectTaskDataSource(anyString(), any(InspectDataSource.class))).thenReturn(new ArrayList<>());
            doNothing().when(logger).warn(anyString(), anyInt());
        }

        void doMock(String status,
                    List<com.tapdata.entity.inspect.InspectTask> ts,
                    boolean isEmpty,
                    int size,
                    int index,
                    com.tapdata.entity.inspect.InspectTask t,
                    String tId) {
            when(inspect.getStatus()).thenReturn(status);
            when(inspect.getTasks()).thenReturn(ts);
            when(tasks.isEmpty()).thenReturn(isEmpty);
            when(tasks.size()).thenReturn(size);
            when(tasks.get(index)).thenReturn(t);
            when(task.getTaskId()).thenReturn(tId);
        }

        void assertVerify(int statusTimes, int getTaskTimes,
                          int isEmptyTimes, int sizeTimes, int getTimes,
                          int warnTimes,
                          int getTaskIdTimes) {
            verify(inspect, times(statusTimes)).getStatus();
            verify(inspect, times(getTaskTimes)).getTasks();
            verify(tasks, times(isEmptyTimes)).isEmpty();
            verify(tasks, times(sizeTimes)).size();
            verify(tasks, times(getTimes)).get(anyInt());
            verify(logger, times(warnTimes)).warn(anyString(), anyInt());
            verify(task, times(getTaskIdTimes)).getTaskId();
            verify(task, times(getTaskIdTimes)).getTarget();
            verify(task, times(getTaskIdTimes)).getSource();
            verify(hashVerifyService, times(getTaskIdTimes*2)).checkRowCountInspectTaskDataSource(anyString(), any(InspectDataSource.class));
        }

        @Test
        void testInspectIsNull() {
            doMock("xxx", tasks, true, 0, 0, task, "id");
            hashVerifyService.checkRowCountInspect(null);
            assertVerify(0, 0, 0, 0, 0, 0, 0);
        }

        @Test
        void testInspectStatusNotScheduling() {
            doMock("xxx", tasks, true, 0, 0, task, "id");
            hashVerifyService.checkRowCountInspect(inspect);
            assertVerify(1, 1, 1, 0, 0, 0, 0);
        }

        @Test
        void testInspectTaskListIsNull() {
            doMock("scheduling", null, true, 0, 0, task, "id");
            hashVerifyService.checkRowCountInspect(inspect);
            assertVerify(1, 1, 0, 0, 0, 0, 0);
        }

        @Test
        void testInspectTaskListIsEmpty() {
            doMock("scheduling", tasks, true, 0, 0, task, "id");
            hashVerifyService.checkRowCountInspect(inspect);
            assertVerify(1, 1, 1, 0, 0, 0, 0);
        }

        @Test
        void testInspectTaskIsNull() {
            doMock("scheduling", tasks, false, 1, 0, null, "id");
            hashVerifyService.checkRowCountInspect(inspect);
            assertVerify(1, 1, 1, 1, 1, 1, 0);
        }

        @Test
        void testInspectTaskIdIsEmpty() {
            doMock("scheduling", tasks, false, 1, 0, task, null);
            hashVerifyService.checkRowCountInspect(inspect);
            assertVerify(1, 1, 1, 1, 1, 0, 1);
        }

        @Test
        void testAllCaseAreSucceed() {
            doMock("scheduling", tasks, false, 1, 0, task, "id");
            hashVerifyService.checkRowCountInspect(inspect);
            assertVerify(1, 1, 1, 1, 1, 0, 1);
        }
    }

    @Nested
    class UpdateStatusTest {
        @Test
        void testNormal() {
            InspectStatus status = mock(InspectStatus.class);
            when(status.getCode()).thenReturn("code");
            doNothing().when(clientMongoOperator).upsert(anyMap(), anyMap(), anyString());
            doCallRealMethod().when(hashVerifyService).updateStatus("id", status, "msg");
            Assertions.assertDoesNotThrow(() -> hashVerifyService.updateStatus("id", status, "msg"));
            verify(status, times(1)).getCode();
            verify(clientMongoOperator, times(1)).upsert(anyMap(), anyMap(), anyString());
        }
    }

    @Nested
    class CheckRowCountInspectTaskDataSourceTest {
        InspectDataSource dataSource;
        @BeforeEach
        void init() {
            dataSource = mock(InspectDataSource.class);

            when(hashVerifyService.checkRowCountInspectTaskDataSource("prefix", dataSource)).thenCallRealMethod();
        }

        void assertVerify(InspectDataSource ds, int times, int size) {
            List<String> list = hashVerifyService.checkRowCountInspectTaskDataSource("prefix", ds);
            Assertions.assertNotNull(list);
            Assertions.assertEquals(size, list.size());
            verify(dataSource, times(times)).getConnectionId();
            verify(dataSource, times(times)).getTable();
        }

        @Test
        void testDataSourceIsNull() {
            when(hashVerifyService.checkRowCountInspectTaskDataSource("prefix", null)).thenCallRealMethod();
            when(dataSource.getConnectionId()).thenReturn("id");
            when(dataSource.getTable()).thenReturn("table");
            assertVerify(null, 0, 1);
        }

        @Test
        void testConnectionIdIsNull() {
            when(dataSource.getConnectionId()).thenReturn(null);
            when(dataSource.getTable()).thenReturn("table");
            assertVerify(dataSource, 1, 1);
        }

        @Test
        void testTableIsNull() {
            when(dataSource.getConnectionId()).thenReturn("id");
            when(dataSource.getTable()).thenReturn(null);
            assertVerify(dataSource, 1, 1);
        }

        @Test
        void testAllCaseIsSucceed() {
            when(dataSource.getConnectionId()).thenReturn("id");
            when(dataSource.getTable()).thenReturn("table");
            assertVerify(dataSource, 1, 0);
        }

        @Test
        void testAllCaseIsFailed() {
            when(dataSource.getConnectionId()).thenReturn(null);
            when(dataSource.getTable()).thenReturn(null);
            assertVerify(dataSource, 1, 2);
        }
    }
}

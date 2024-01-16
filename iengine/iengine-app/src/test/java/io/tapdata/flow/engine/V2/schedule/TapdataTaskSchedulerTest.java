package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryFactory;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TapdataTaskSchedulerTest {
    private TapdataTaskScheduler tapdataTaskScheduler;
    private Map<String, Long> taskRetryTimeMap;
    private Map<String, TaskClient<TaskDto>> taskClientMap;
    @BeforeEach
    void buildTapdataTaskScheduler() throws NoSuchFieldException, IllegalAccessException {
        tapdataTaskScheduler = mock(TapdataTaskScheduler.class);
        Class<TapdataTaskScheduler> clazz = TapdataTaskScheduler.class;
        Field taskRetryTimeMapField = clazz.getDeclaredField("taskRetryTimeMap");
        taskRetryTimeMapField.setAccessible(true);
        taskRetryTimeMap = (Map<String, Long>)(taskRetryTimeMapField.get(clazz));
        taskRetryTimeMap.put("111",1L);
        taskRetryTimeMap.put("222",2L);
        taskClientMap = mock(Map.class);
        ReflectionTestUtils.setField(tapdataTaskScheduler,"taskClientMap",taskClientMap);
    }
    @Nested
    class ResetTaskRetryServiceIfNeedTest{
        @DisplayName("test reset task retry service if need normal")
        @Test
        void test1(){
            TaskRetryFactory factory = mock(TaskRetryFactory.class);
            try (MockedStatic<TaskRetryFactory> mb = Mockito
                    .mockStatic(TaskRetryFactory.class)) {
                mb.when(TaskRetryFactory::getInstance).thenReturn(factory);
                ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
                try (MockedStatic<ObsLoggerFactory> of = Mockito
                        .mockStatic(ObsLoggerFactory.class)) {
                    mb.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                    when(obsLoggerFactory.getObsLogger(anyString())).thenReturn(mock(ObsLogger.class));
                    TaskRetryService taskRetryService = mock(TaskRetryService.class);
                    when(factory.getTaskRetryService(anyString())).thenReturn(Optional.ofNullable(taskRetryService));
                    TaskClient taskDtoTaskClient = mock(TaskClient.class);
                    when(taskClientMap.get(anyString())).thenReturn(taskDtoTaskClient);
                    TaskDto taskDto = mock(TaskDto.class);
                    when(taskDtoTaskClient.getTask()).thenReturn(taskDto);
                    when(taskDto.getName()).thenReturn("task 1");
                    doCallRealMethod().when(tapdataTaskScheduler).resetTaskRetryServiceIfNeed();
                    tapdataTaskScheduler.resetTaskRetryServiceIfNeed();
                    assertEquals(0, taskRetryTimeMap.size());
                }
            }
        }
        @DisplayName("test reset task retry service if need with null task retry service")
        @Test
        void test2(){
            doCallRealMethod().when(tapdataTaskScheduler).resetTaskRetryServiceIfNeed();
            tapdataTaskScheduler.resetTaskRetryServiceIfNeed();
            assertEquals(2, taskRetryTimeMap.size());
        }

    }
}

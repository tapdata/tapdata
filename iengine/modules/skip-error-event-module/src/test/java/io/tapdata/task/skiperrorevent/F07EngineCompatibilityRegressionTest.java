package io.tapdata.task.skiperrorevent;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.aspect.SkipErrorDataAspect;
import io.tapdata.aspect.SkipErrorProcessAspect;
import io.tapdata.aspect.WriteRecordFuncAspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import io.tapdata.dql.reporter.DqlEventReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class F07EngineCompatibilityRegressionTest {

    private SkipErrorEventAspectTask task;
    private DqlEventReporter reporter;

    @BeforeEach
    void setUp() {
        task = new SkipErrorEventAspectTask();
        reporter = mock(DqlEventReporter.class);
        ReflectionTestUtils.setField(task, "taskId", "task-1");
        ReflectionTestUtils.setField(task, "dqlEventReporter", reporter);
        ReflectionTestUtils.setField(task, "log", mock(Log.class));

        TaskDto.SkipErrorEvent skipData = new TaskDto.SkipErrorEvent();
        skipData.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.SkipData);
        ReflectionTestUtils.setField(task, "skipErrorEvent", skipData);
    }

    @Test
    void disablingDqlKeepsAllCaptureHandlersOnTheLegacyPath() {
        ReflectionTestUtils.setField(task, "dqlRuntimeConfig",
                DqlRuntimeConfig.fromMap(Map.of(DqlRuntimeConfig.EVENT_ENABLED, false)));
        Function<SkipErrorDataAspect, AspectInterceptResult> staleHandler = mock(Function.class);
        ReflectionTestUtils.setField(task, "skipErrorDataNoeAspect", staleHandler);

        assertNull(task.skipErrorDataNoeAspectHandle(mock(SkipErrorDataAspect.class)));

        WriteRecordFuncAspect writeAspect = new WriteRecordFuncAspect()
                .recordEvents(List.of(insertEvent(1)))
                .table(new TapTable("orders"))
                .start();
        assertNull(task.writeRecordFuncAspectHandle(writeAspect));
        assertTrue(writeAspect.getConsumers().isEmpty());

        TapdataEvent inputEvent = new TapdataEvent();
        inputEvent.setTapEvent(insertEvent(2));
        SkipErrorProcessAspect processAspect = new SkipErrorProcessAspect()
                .inputEvent(inputEvent)
                .error(new IllegalArgumentException("legacy failure"));
        assertNull(task.skipErrorProcessAspectHandle(processAspect));

        verifyNoInteractions(staleHandler, reporter);
    }

    @Test
    void tableSkipModesDoNotEnterDqlDataCapture() {
        for (TaskDto.SkipErrorEvent.ErrorMode mode : List.of(
                TaskDto.SkipErrorEvent.ErrorMode.SkipTable,
                TaskDto.SkipErrorEvent.ErrorMode.SkipTableForMigrateSnapshot,
                TaskDto.SkipErrorEvent.ErrorMode.Disable)) {
            TaskDto.SkipErrorEvent config = new TaskDto.SkipErrorEvent();
            config.setErrorMode(mode);
            ReflectionTestUtils.setField(task, "skipErrorEvent", config);
            assertNull(task.skipErrorDataNoeAspectHandle(mock(SkipErrorDataAspect.class)), mode.name());
        }
        verifyNoInteractions(reporter);
    }

    @Test
    void dqlTaskModeEnablesDqlCaptureHandlers() {
        TaskDto.SkipErrorEvent config = new TaskDto.SkipErrorEvent();
        config.setErrorMode("DQL");
        ReflectionTestUtils.setField(task, "skipErrorEvent", config);
        ReflectionTestUtils.setField(task, "dqlRuntimeConfig",
                DqlRuntimeConfig.fromMap(Map.of(DqlRuntimeConfig.EVENT_ENABLED, true)));

        AspectInterceptResult expected = new AspectInterceptResult().intercepted(true);
        ReflectionTestUtils.setField(task, "skipErrorDataNoeAspect",
                (Function<SkipErrorDataAspect, AspectInterceptResult>) ignored -> expected);

        assertSame(expected, task.skipErrorDataNoeAspectHandle(mock(SkipErrorDataAspect.class)));
    }

    private TapRecordEvent insertEvent(int id) {
        return TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", id));
    }
}

package io.tapdata.task.skiperrorevent;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import io.tapdata.aspect.SkipErrorDataAspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertSame;

class DqlTaskModeEngineContractTest {

    @Test
    void dqlTaskModeEnablesTheTargetCaptureHandler() {
        SkipErrorEventAspectTask task = new SkipErrorEventAspectTask();
        TaskDto.SkipErrorEvent config = new TaskDto.SkipErrorEvent();
        config.setErrorMode("DQL");
        ReflectionTestUtils.setField(task, "skipErrorEvent", config);
        ReflectionTestUtils.setField(task, "dqlRuntimeConfig",
                DqlRuntimeConfig.fromMap(Map.of(DqlRuntimeConfig.EVENT_ENABLED, true)));

        AspectInterceptResult expected = new AspectInterceptResult().intercepted(true);
        ReflectionTestUtils.setField(task, "skipErrorDataNoeAspect",
                (Function<SkipErrorDataAspect, AspectInterceptResult>) ignored -> expected);

        assertSame(expected, task.skipErrorDataNoeAspectHandle(new SkipErrorDataAspect()));
    }
}

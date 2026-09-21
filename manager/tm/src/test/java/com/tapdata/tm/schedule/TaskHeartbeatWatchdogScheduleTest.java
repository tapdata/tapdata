package com.tapdata.tm.schedule;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.TaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TaskHeartbeatWatchdogScheduleTest {
    private TaskHeartbeatWatchdogSchedule schedule;
    private TaskService service;
    private TaskDto task;
    @BeforeEach void setup() {
        schedule = new TaskHeartbeatWatchdogSchedule();
        service = mock(TaskService.class);
        ReflectionTestUtils.setField(schedule, "taskService", service);
        when(service.update(any(Query.class), any(Update.class))).thenReturn(UpdateResult.acknowledged(0, 0L, null));
        task = new TaskDto();
        task.setId(new ObjectId());
        task.setAgentId("agent");
        task.setStatus(TaskDto.STATUS_RUNNING);
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAttrs(new HashMap<>(Map.of("heartbeatWatchdog", Map.of("enabled", true, "units", List.of("a"), "timeoutMs", 60_000L, "graceMs", 60_000L),
                "syncProgress", Map.of("a", "{\"syncStage\":\"CDC\",\"streamOffset\":\"1\"}"))));
    }
    private Map<?, ?> updatedRecovery() {
        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(service).update(any(Query.class), update.capture());
        return (Map<?, ?>) ((Map<?, ?>) update.getValue().getUpdateObject().get("$set")).get("attrs.heartbeatRecovery");
    }

    @Test void fallbackRequestsSameEngineRecoveryWithoutRescheduling() {
        schedule.inspect(task, 0);
        schedule.inspect(task, 60_000);
        Map<?, ?> recovery = updatedRecovery();
        assertEquals("REQUESTED", recovery.get("state"));
        assertEquals("TM", recovery.get("origin"));
    }

    @Test void stalledStopBecomesBlockedRatherThanSecondRestart() {
        task.getAttrs().put("heartbeatRecovery", Map.of("state", "STOPPING", "claimedAt", 1L));
        schedule.inspect(task, 120_002);
        assertEquals("BLOCKED", updatedRecovery().get("state"));
    }

    @Test void blockedRequestIsNotReissued() {
        task.getAttrs().put("heartbeatRecovery", Map.of("state", "BLOCKED"));
        schedule.inspect(task, 0);
        schedule.inspect(task, 600_000);
        verify(service, never()).update(any(Query.class), any(Update.class));
    }

    @Test void freshEngineGenerationGetsGrace() {
        schedule.inspect(task, 0);
        task.getAttrs().put("heartbeatHealth", Map.of("runId", "new-run"));
        schedule.inspect(task, 600_000);
        verify(service, never()).update(any(Query.class), any(Update.class));
    }

    @Test void verificationDoesNotTreatUnchangedOffsetAsRecovery() {
        task.getAttrs().put("heartbeatRecovery", Map.of("state", "VERIFYING", "startedAt", 1L));
        schedule.inspect(task, 120_002);
        assertEquals("FAILED", updatedRecovery().get("state"));
    }

    @Test void resumedCheckpointCancelsExpiredUnclaimedRequest() {
        task.getAttrs().put("heartbeatRecovery", Map.of("state", "REQUESTED", "requestedAt", 1L,
                "fingerprints", Map.of("a", "an-older-offset-digest")));
        schedule.inspect(task, 120_002);
        assertEquals("CANCELLED", updatedRecovery().get("state"));
    }
}

package io.tapdata.flow.engine.V2.monitor.heartbeat;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class HeartbeatProgressRegistryTest {
    private TaskDto task() {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAttrs(new HashMap<>(Map.of("heartbeatWatchdog", Map.of("enabled", true, "units", List.of("edge")))));
        return task;
    }

    @Test void onlySuccessfulPersistenceHookUpdatesCheckpointNotSourceHeartbeat() {
        TaskDto task = task();
        var state = HeartbeatProgressRegistry.open(task);
        try {
            HeartbeatProgressRegistry.sourceHeartbeat(task, "source");
            assertFalse(state.sourceHeartbeats.isEmpty());
            assertTrue(state.progress.isEmpty());
            assertEquals(0, state.lastPersistedAt);
            HeartbeatProgressRegistry.persisted(task, Map.of("edge", "checkpoint"));
            assertEquals("checkpoint", state.progress.get("edge"));
            assertTrue(state.lastPersistedAt > 0);
        } finally { HeartbeatProgressRegistry.close(task, state); }
    }

    @Test void delayedOldMonitorCloseCannotRemoveNewRun() {
        TaskDto task = task();
        var old = HeartbeatProgressRegistry.open(task);
        var replacement = HeartbeatProgressRegistry.open(task);
        try {
            HeartbeatProgressRegistry.close(task, old);
            assertSame(replacement, HeartbeatProgressRegistry.get(task.getId().toHexString()));
            assertNotEquals(old.runId, replacement.runId);
        } finally { HeartbeatProgressRegistry.close(task, replacement); }
    }
}

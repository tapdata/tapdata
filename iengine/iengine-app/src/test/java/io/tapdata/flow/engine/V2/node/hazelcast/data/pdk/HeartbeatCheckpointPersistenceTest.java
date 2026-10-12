package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.monitor.heartbeat.HeartbeatProgressRegistry;
import io.tapdata.observable.logging.ObsLogger;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HeartbeatCheckpointPersistenceTest {
    private HazelcastTargetPdkBaseNode node;
    private ClientMongoOperator mongo;
    private TaskDto task;
    private HeartbeatProgressRegistry.State state;

    @BeforeEach void setup() {
        task = new TaskDto();
        task.setId(new ObjectId());
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAttrs(new HashMap<>(Map.of("heartbeatWatchdog", Map.of("enabled", true, "units", List.of("[\"source\",\"target\"]")))));
        state = HeartbeatProgressRegistry.open(task);
        node = mock(HazelcastTargetPdkBaseNode.class);
        mongo = mock(ClientMongoOperator.class);
        // Build the context through its existing builder to exercise the real snapshot method.
        var context = new com.tapdata.entity.task.context.DataProcessorContext.DataProcessorContextBuilder()
                .withTaskDto(task).build();
        ReflectionTestUtils.setField(node, "dataProcessorContext", context);
        ReflectionTestUtils.setField(node, "clientMongoOperator", mongo);
        ReflectionTestUtils.setField(node, "obsLogger", mock(ObsLogger.class));
        ReflectionTestUtils.setField(node, "saveSnapshotLock", new Object());
        ReflectionTestUtils.setField(node, "flushOffset", new AtomicBoolean(true));
        ReflectionTestUtils.setField(node, "uploadDagService", new AtomicBoolean(false));
        SyncProgress progress = new SyncProgress();
        progress.setSyncStage("CDC");
        progress.setStreamOffset("checkpoint");
        ReflectionTestUtils.setField(node, "syncProgressMap", Map.of("source,target", progress));
        doCallRealMethod().when(node).saveToSnapshot();
    }

    @AfterEach void cleanup() { HeartbeatProgressRegistry.close(task, state); }

    @Test void publishesProgressOnlyAfterTmAcknowledgesPersistence() {
        doAnswer(invocation -> {
            assertEquals(0, state.lastPersistedAt);
            assertTrue(state.progress.isEmpty());
            return null;
        }).when(mongo).insertOne(any(), anyString());
        assertTrue(node.saveToSnapshot());
        assertTrue(state.lastPersistedAt > 0);
        assertFalse(state.progress.isEmpty());
    }

    @Test void failedPersistenceNeverRefreshesWatchdog() {
        doThrow(new RuntimeException("TM unavailable")).when(mongo).insertOne(any(), anyString());
        assertFalse(node.saveToSnapshot());
        assertEquals(0, state.lastPersistedAt);
        assertTrue(state.progress.isEmpty());
    }
}

package io.tapdata.flow.engine.V2.monitor.heartbeat;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.heartbeat.HeartbeatWatchdog;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.TaskClient;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Deterministic vertical coverage for the engine-side heartbeat recovery path.
 *
 * <p>The registry, detector, watchdog and CAS protocol are real. Mongo and the
 * scheduler are deliberately small in-memory adapters so this test does not
 * depend on a running TM, MongoDB, connector or binlog implementation.</p>
 */
class HeartbeatProgressWatchdogIntegrationTest {
    private HeartbeatProgressWatchdog watchdog;
    private TapdataTaskScheduler scheduler;
    private ClientMongoOperator mongo;
    private TaskClient<TaskDto> client;
    private TaskDto task;
    private HeartbeatProgressRegistry.State local;
    private AtomicLong observationClock;
    private MockedStatic<io.tapdata.observable.logging.ObsLoggerFactory> loggerFactory;

    @BeforeEach
    void setUp() {
        loggerFactory = mockStatic(io.tapdata.observable.logging.ObsLoggerFactory.class);
        loggerFactory.when(io.tapdata.observable.logging.ObsLoggerFactory::getInstance)
                .thenReturn(mock(io.tapdata.observable.logging.ObsLoggerFactory.class));

        task = task(checkpoint(1));
        client = mock(TaskClient.class);
        when(client.getTask()).thenReturn(task);

        scheduler = mock(TapdataTaskScheduler.class);
        mongo = new InMemoryTaskMongo(task).operator();
        watchdog = new HeartbeatProgressWatchdog();
        ReflectionTestUtils.setField(watchdog, "scheduler", scheduler);
        ReflectionTestUtils.setField(watchdog, "mongo", mongo);

        long clockStart = HeartbeatProgressRegistry.monotonicMillis();
        observationClock = new AtomicLong(clockStart);
        watchdog = spy(watchdog);
        doAnswer(invocation -> observationClock.get()).when(watchdog).observationTime();
        local = HeartbeatProgressRegistry.open(task);
    }

    @AfterEach
    void tearDown() {
        watchdog.close();
        HeartbeatProgressRegistry.close(task, HeartbeatProgressRegistry.get(task.getId().toHexString()));
        loggerFactory.close();
    }

    @Test
    void stalledCheckpointRunsThroughObservationCasRestartAndRecovery() throws Exception {
        String oldRunId = local.runId;
        inspectAt(0);

        when(scheduler.recoverHeartbeatTask(eq(client), any(BooleanSupplier.class), any(BooleanSupplier.class)))
                .thenAnswer(invocation -> {
                    BooleanSupplier claim = invocation.getArgument(1);
                    BooleanSupplier allowRestart = invocation.getArgument(2);
                    assertTrue(claim.getAsBoolean(), "watchdog must claim the recovery with a CAS");
                    assertTrue(allowRestart.getAsBoolean(), "watchdog must fence before restarting");

                    // Model the scheduler attaching a replacement run after the old run was
                    // stopped. The new registry state is what a real engine start creates.
                    HeartbeatProgressRegistry.close(task, local);
                    task.setTaskRecordId("record-2");
                    task.setLastStartDate(2L);
                    task.getAttrs().put("syncProgress", Map.of("edge", checkpoint(2)));
                    local = HeartbeatProgressRegistry.open(task);
                    HeartbeatProgressRegistry.persisted(task, Map.of("edge", checkpoint(2)));
                    // Keep the persistence evidence strictly after startedAt even on a
                    // millisecond-resolution clock where both callbacks can share a tick.
                    local.lastPersistedAt = System.currentTimeMillis() + 1;
                    return true;
                });

        observationClock.addAndGet(HeartbeatWatchdog.SCAN_MS + HeartbeatWatchdog.timeout(task)
                + HeartbeatWatchdog.grace(task) + 1_000);
        Map<String, String> stuck = local.detector.inspect(task, local.progress, observationClock.get());
        Map<String, Object> request = HeartbeatWatchdog.request(task, stuck, "ENGINE", System.currentTimeMillis());
        assertNotNull(request);
        task.getAttrs().put(HeartbeatWatchdog.RECOVERY, request);
        watchdog.recover(client, task, request, local);
        assertEquals("VERIFYING", recovery().get("state"));

        inspectAt(1);
        assertEquals("RECOVERED", recovery().get("state"));

        Map<String, Object> health = health();
        assertEquals("OBSERVING", health.get("state"));
        assertEquals(local.runId, health.get("runId"));
        assertNotEquals(oldRunId, health.get("runId"));
        assertEquals(List.of(), health.get("stuckUnits"));
        verify(scheduler, timeout(2_000)).recoverHeartbeatTask(eq(client), any(), any());
    }

    @Test
    void advancingCheckpointStaysObservingAndNeverRequestsRecovery() {
        inspectAt(0);

        task.getAttrs().put("syncProgress", Map.of("edge", checkpoint(2)));
        HeartbeatProgressRegistry.persisted(task, Map.of("edge", checkpoint(2)));
        inspectAt(HeartbeatWatchdog.SCAN_MS + HeartbeatWatchdog.timeout(task)
                + HeartbeatWatchdog.grace(task) + 1_000);

        assertEquals("OBSERVING", health().get("state"));
        assertEquals(List.of(), health().get("stuckUnits"));
        assertTrue(recovery().isEmpty());
        verifyNoInteractions(scheduler);
    }

    private void inspectAt(long elapsed) {
        observationClock.set(observationClock.get() + elapsed);
        watchdog.inspect(client);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> health() {
        return (Map<String, Object>) task.getAttrs().get(HeartbeatWatchdog.HEALTH);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> recovery() {
        Object value = task.getAttrs().get(HeartbeatWatchdog.RECOVERY);
        return value instanceof Map ? (Map<String, Object>) value : Map.of();
    }

    private static TaskDto task(String progress) {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setStatus(TaskDto.STATUS_RUNNING);
        task.setAgentId("agent");
        task.setTaskRecordId("record-1");
        task.setLastStartDate(1L);
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAttrs(new HashMap<>(Map.of(
                HeartbeatWatchdog.CONFIG, Map.of("enabled", true, "units", List.of("edge"),
                        "timeoutMs", 60_000L, "graceMs", 15_000L),
                "syncProgress", Map.of("edge", progress))));
        return task;
    }

    private static String checkpoint(int offset) {
        return "{\"syncStage\":\"CDC\",\"streamOffset\":\"" + offset + "\"}";
    }

    /** A minimal Mongo adapter that applies only the update/CAS operations used by the watchdog. */
    private static final class InMemoryTaskMongo {
        private final TaskDto task;

        private InMemoryTaskMongo(TaskDto task) {
            this.task = task;
        }

        private ClientMongoOperator operator() {
            ClientMongoOperator operator = mock(ClientMongoOperator.class);
            try {
                when(operator.findOne(any(Query.class), anyString(), eq(TaskDto.class)))
                        .thenAnswer(invocation -> task);
                when(operator.update(any(Query.class), any(Update.class), anyString()))
                        .thenAnswer(invocation -> apply(invocation.getArgument(0), invocation.getArgument(1)));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            return operator;
        }

        private synchronized UpdateResult apply(Query query, Update update) {
            if (!matches(query.getQueryObject())) return UpdateResult.acknowledged(0, 0L, null);
            Document set = update.getUpdateObject().get("$set", Document.class);
            for (Map.Entry<String, Object> entry : set.entrySet()) {
                if ("attrs.heartbeatHealth".equals(entry.getKey())) {
                    task.getAttrs().put(HeartbeatWatchdog.HEALTH, copyMap(entry.getValue()));
                } else if ("attrs.heartbeatRecovery".equals(entry.getKey())) {
                    task.getAttrs().put(HeartbeatWatchdog.RECOVERY, copyMap(entry.getValue()));
                }
            }
            return UpdateResult.acknowledged(1, 1L, null);
        }

        private boolean matches(Document query) {
            return task.getId().equals(query.get("_id"))
                    && TaskDto.STATUS_RUNNING.equals(query.getString("status"))
                    && task.getAgentId().equals(query.getString("agentId"))
                    && task.getTaskRecordId().equals(query.get("taskRecordId"))
                    && task.getLastStartDate().equals(query.get("lastStartDate"))
                    && matchesRecovery(query);
        }

        private boolean matchesRecovery(Document query) {
            Object expectedId = query.get(HeartbeatWatchdog.RECOVERY_PATH + ".id");
            Object expectedState = query.get(HeartbeatWatchdog.RECOVERY_PATH + ".state");
            if (expectedId == null && expectedState == null) return true;
            Map<String, Object> recovery = HeartbeatWatchdog.attr(task, HeartbeatWatchdog.RECOVERY);
            return expectedId != null && expectedId.equals(recovery.get("id"))
                    && expectedState != null && expectedState.equals(recovery.get("state"));
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> copyMap(Object value) {
            if (!(value instanceof Map)) return new LinkedHashMap<>();
            return new LinkedHashMap<>((Map<String, Object>) value);
        }
    }
}

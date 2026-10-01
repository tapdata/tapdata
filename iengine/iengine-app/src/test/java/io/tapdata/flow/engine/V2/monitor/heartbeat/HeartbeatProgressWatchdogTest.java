package io.tapdata.flow.engine.V2.monitor.heartbeat;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.heartbeat.HeartbeatWatchdog;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.TaskClient;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HeartbeatProgressWatchdogTest {
    private HeartbeatProgressWatchdog watchdog;
    private TapdataTaskScheduler scheduler;
    private ClientMongoOperator mongo;
    private TaskDto task;
    private TaskClient<TaskDto> client;
    private HeartbeatProgressRegistry.State local;
    private Map<String, Object> request;
    private org.mockito.MockedStatic<io.tapdata.observable.logging.ObsLoggerFactory> loggerFactory;

    @BeforeEach void setup() throws Exception {
        loggerFactory = mockStatic(io.tapdata.observable.logging.ObsLoggerFactory.class);
        loggerFactory.when(io.tapdata.observable.logging.ObsLoggerFactory::getInstance)
                .thenReturn(mock(io.tapdata.observable.logging.ObsLoggerFactory.class));
        watchdog = new HeartbeatProgressWatchdog();
        scheduler = mock(TapdataTaskScheduler.class);
        mongo = mock(ClientMongoOperator.class);
        ReflectionTestUtils.setField(watchdog, "scheduler", scheduler);
        ReflectionTestUtils.setField(watchdog, "mongo", mongo);
        task = new TaskDto();
        task.setId(new ObjectId());
        task.setStatus(TaskDto.STATUS_RUNNING);
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAttrs(new HashMap<>(Map.of("heartbeatWatchdog", Map.of("enabled", true, "units", List.of("a")),
                "syncProgress", Map.of("a", "{\"syncStage\":\"CDC\",\"streamOffset\":\"1\"}"))));
        local = HeartbeatProgressRegistry.open(task);
        request = HeartbeatWatchdog.request(task, HeartbeatWatchdog.fingerprints(task, HeartbeatWatchdog.attr(task, "syncProgress")), "TM", System.currentTimeMillis());
        task.getAttrs().put("heartbeatRecovery", request);
        client = mock(TaskClient.class);
        when(client.getTask()).thenReturn(task);
        when(mongo.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(task);
        when(mongo.update(any(Query.class), any(Update.class), anyString())).thenReturn(UpdateResult.acknowledged(1, 1L, null));
    }

    @AfterEach void cleanup() {
        watchdog.close();
        HeartbeatProgressRegistry.close(task, local);
        loggerFactory.close();
    }

    @Test void revalidatesProgressBeforeClaimingQueuedRecovery() throws Exception {
        task.getAttrs().put("syncProgress", Map.of("a", "{\"syncStage\":\"CDC\",\"streamOffset\":\"2\"}"));
        when(scheduler.recoverHeartbeatTask(eq(client), any(), any())).thenAnswer(invocation -> {
            assertFalse(((BooleanSupplier) invocation.getArgument(1)).getAsBoolean());
            return false;
        });
        watchdog.recover(client, task, request, local);
        assertEquals("CANCELLED", lastRecoveryUpdate().get("state"));
    }

    @Test void lostCasDoesNotAuthorizeStopping() throws Exception {
        when(mongo.update(any(Query.class), any(Update.class), anyString())).thenReturn(UpdateResult.acknowledged(0, 0L, null));
        when(scheduler.recoverHeartbeatTask(eq(client), any(), any())).thenAnswer(invocation -> {
            assertFalse(((BooleanSupplier) invocation.getArgument(1)).getAsBoolean());
            return false;
        });
        watchdog.recover(client, task, request, local);
    }

    @Test void stopExceptionBlocksFurtherAttempts() throws Exception {
        when(scheduler.recoverHeartbeatTask(eq(client), any(), any())).thenAnswer(invocation -> {
            // The claim succeeded (the CAS moved REQUESTED -> STOPPING for real, as replace()
            // inside the claim lambda would have persisted) before the stop itself threw.
            assertTrue(((BooleanSupplier) invocation.getArgument(1)).getAsBoolean());
            Map<String, Object> stopping = new HashMap<>(request);
            stopping.put("state", "STOPPING");
            task.getAttrs().put("heartbeatRecovery", stopping);
            throw new IllegalStateException("stop failed");
        });
        watchdog.recover(client, task, request, local);
        assertEquals("BLOCKED", lastRecoveryUpdate().get("state"));
    }

    @Test void unclaimedStopExceptionLeavesRequestRetryable() throws Exception {
        // The claim itself never confirmed a stop was attempted (e.g. a transient TM HTTP
        // failure while reading before claiming), so the persisted state is still REQUESTED.
        // There is nothing to fence, so this must NOT be permanently BLOCKED.
        when(scheduler.recoverHeartbeatTask(eq(client), any(), any())).thenAnswer(invocation -> {
            throw new IllegalStateException("claim read failed");
        });
        watchdog.recover(client, task, request, local);
        assertEquals("REQUESTED", ((Map<?, ?>) task.getAttrs().get("heartbeatRecovery")).get("state"));
    }

    @Test void startExceptionBlocksCurrentVerifyingRecovery() throws Exception {
        when(scheduler.recoverHeartbeatTask(eq(client), any(), any())).thenAnswer(invocation -> {
            assertTrue(((BooleanSupplier) invocation.getArgument(1)).getAsBoolean());
            assertTrue(((BooleanSupplier) invocation.getArgument(2)).getAsBoolean());
            Map<String, Object> verifying = new HashMap<>(request);
            verifying.put("state", "VERIFYING");
            task.getAttrs().put("heartbeatRecovery", verifying);
            throw new IllegalStateException("start failed");
        });
        watchdog.recover(client, task, request, local);
        assertEquals("BLOCKED", lastRecoveryUpdate().get("state"));
    }

    @Test void successfulRecoveryEntersVerificationAndThenRecovers() throws Exception {
        when(scheduler.recoverHeartbeatTask(eq(client), any(), any())).thenAnswer(invocation -> {
            assertTrue(((BooleanSupplier) invocation.getArgument(1)).getAsBoolean());
            assertTrue(((BooleanSupplier) invocation.getArgument(2)).getAsBoolean());
            Map<String, Object> verifying = new HashMap<>(request);
            verifying.put("state", "VERIFYING");
            verifying.put("startedAt", System.currentTimeMillis());
            task.getAttrs().put("heartbeatRecovery", verifying);
            return true;
        });
        watchdog.recover(client, task, request, local);
        assertEquals("VERIFYING", ((Map<?, ?>) task.getAttrs().get("heartbeatRecovery")).get("state"));

        local.lastPersistedAt = System.currentTimeMillis() + 1;
        task.getAttrs().put("syncProgress", Map.of("a", "{\"syncStage\":\"CDC\",\"streamOffset\":\"2\"}"));
        watchdog.inspect(client);
        assertEquals("RECOVERED", lastRecoveryUpdate().get("state"));
    }

    @Test void budgetExhaustionMovesQueuedRecoveryToCircuitOpen() {
        Map<String, Object> exhausted = new HashMap<>(request);
        long now = System.currentTimeMillis();
        exhausted.put("attempts", List.of(now - 3_000L, now - 2_000L, now - 1_000L));
        task.getAttrs().put("heartbeatRecovery", exhausted);
        watchdog.recover(client, task, exhausted, local);
        assertEquals("CIRCUIT_OPEN", lastRecoveryUpdate().get("state"));
    }

    @Test void scannerCanBlockHungRecoveryWithoutWaitingForTm() {
        Map<String, Object> stopping = new HashMap<>(request);
        stopping.put("state", "STOPPING");
        stopping.put("claimedAt", System.currentTimeMillis() - HeartbeatWatchdog.RECOVERY_TIMEOUT_MS - 1);
        task.getAttrs().put("heartbeatRecovery", stopping);
        watchdog.inspect(client);
        assertEquals("BLOCKED", lastRecoveryUpdate().get("state"));
    }

    @Test void circuitOpenWithinAttemptWindowIsNotRewrittenEveryScan() {
        // Unlike BLOCKED, CIRCUIT_OPEN must not early-return unconditionally in inspect() (that
        // would make it permanent, diverging from TaskHeartbeatWatchdogSchedule on the TM side,
        // which re-requests once the 1-hour attempt window ages out). But while still within the
        // window, nothing changed, so this must stay a no-op: no repeated write/log every 15s.
        Map<String, Object> circuitOpen = new HashMap<>(request);
        long now = System.currentTimeMillis();
        circuitOpen.put("state", "CIRCUIT_OPEN");
        circuitOpen.put("attempts", List.of(now - 3_000L, now - 2_000L, now - 1_000L));
        task.getAttrs().put("heartbeatRecovery", circuitOpen);
        watchdog.inspect(client);
        assertTrue(recoveryUpdates().isEmpty(),
                "CIRCUIT_OPEN must not be rewritten while still inside the 1-hour attempt window");
    }

    private Map<?, ?> lastRecoveryUpdate() {
        List<Map<?, ?>> updates = recoveryUpdates();
        return updates.get(updates.size() - 1);
    }

    private List<Map<?, ?>> recoveryUpdates() {
        ArgumentCaptor<Update> updates = ArgumentCaptor.forClass(Update.class);
        verify(mongo, atLeastOnce()).update(any(Query.class), updates.capture(), anyString());
        List<Map<?, ?>> result = new ArrayList<>();
        for (Update update : updates.getAllValues()) {
            Object recovery = ((Map<?, ?>) update.getUpdateObject().get("$set")).get("attrs.heartbeatRecovery");
            if (recovery != null) result.add((Map<?, ?>) recovery);
        }
        return result;
    }
}

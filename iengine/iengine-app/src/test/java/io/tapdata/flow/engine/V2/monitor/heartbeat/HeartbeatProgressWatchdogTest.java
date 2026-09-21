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
            assertTrue(((BooleanSupplier) invocation.getArgument(1)).getAsBoolean());
            throw new IllegalStateException("stop failed");
        });
        watchdog.recover(client, task, request, local);
        assertEquals("BLOCKED", lastRecoveryUpdate().get("state"));
    }

    @Test void scannerCanBlockHungRecoveryWithoutWaitingForTm() {
        Map<String, Object> stopping = new HashMap<>(request);
        stopping.put("state", "STOPPING");
        stopping.put("claimedAt", System.currentTimeMillis() - HeartbeatWatchdog.RECOVERY_TIMEOUT_MS - 1);
        task.getAttrs().put("heartbeatRecovery", stopping);
        watchdog.inspect(client);
        assertEquals("BLOCKED", lastRecoveryUpdate().get("state"));
    }

    private Map<?, ?> lastRecoveryUpdate() {
        ArgumentCaptor<Update> updates = ArgumentCaptor.forClass(Update.class);
        verify(mongo, atLeastOnce()).update(any(Query.class), updates.capture(), anyString());
        List<Update> values = updates.getAllValues();
        return (Map<?, ?>) ((Map<?, ?>) values.get(values.size() - 1).getUpdateObject().get("$set")).get("attrs.heartbeatRecovery");
    }
}

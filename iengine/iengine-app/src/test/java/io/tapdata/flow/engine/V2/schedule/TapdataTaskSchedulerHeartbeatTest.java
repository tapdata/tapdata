package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.TaskClient;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TapdataTaskSchedulerHeartbeatTest {
    private TapdataTaskScheduler scheduler;
    private TaskClient<TaskDto> client;
    private TaskDto task;
    private Map<String, TaskClient<TaskDto>> clients;
    private BooleanSupplier claim;
    private BooleanSupplier restart;

    @BeforeEach void setup() {
        scheduler = mock(TapdataTaskScheduler.class, CALLS_REAL_METHODS);
        ReflectionTestUtils.setField(scheduler, "logger", mock(org.apache.logging.log4j.Logger.class));
        task = new TaskDto();
        task.setId(new ObjectId());
        task.setStatus(TaskDto.STATUS_RUNNING);
        task.setAgentId("agent");
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAttrs(new HashMap<>(Map.of("heartbeatWatchdog", Map.of("enabled", true, "units", List.of("edge")))));
        client = mock(TaskClient.class);
        when(client.getTask()).thenReturn(task);
        doAnswer(invocation -> client.stop()).when(scheduler).stopHeartbeatTask(client);
        clients = new ConcurrentHashMap<>();
        clients.put(task.getId().toHexString(), client);
        ReflectionTestUtils.setField(scheduler, "taskClientMap", clients);
        doReturn(task).when(scheduler).findHeartbeatRecoveryTask(anyString());
        doAnswer(invocation -> { clients.put(task.getId().toHexString(), mock(TaskClient.class)); return null; })
                .when(scheduler).startTask(any());
        claim = mock(BooleanSupplier.class);
        restart = mock(BooleanSupplier.class);
        when(claim.getAsBoolean()).thenReturn(true);
        when(restart.getAsBoolean()).thenReturn(true);
    }

    @Test void neverStartsWhenStopIsUnconfirmed() throws Exception {
        when(client.stop()).thenReturn(false);
        assertFalse(scheduler.recoverHeartbeatTask(client, claim, restart));
        verify(restart, never()).getAsBoolean();
        verify(scheduler, never()).startTask(any());
        assertSame(client, clients.get(task.getId().toHexString()));
    }

    @Test void confirmedStopPrecedesRestartAndUsesFreshTask() throws Exception {
        when(client.stop()).thenReturn(true);
        assertTrue(scheduler.recoverHeartbeatTask(client, claim, restart));
        var order = inOrder(claim, client, restart, scheduler);
        order.verify(claim).getAsBoolean();
        order.verify(client).stop();
        order.verify(restart).getAsBoolean();
        order.verify(scheduler).startTask(task);
    }

    @Test void lostClaimCannotStopOrStart() throws Exception {
        when(claim.getAsBoolean()).thenReturn(false);
        assertFalse(scheduler.recoverHeartbeatTask(client, claim, restart));
        verify(client, never()).stop();
        verify(scheduler, never()).startTask(any());
    }

    @Test void timeoutFencingPreventsRestartAfterSlowStopReturns() throws Exception {
        when(client.stop()).thenReturn(true);
        when(restart.getAsBoolean()).thenReturn(false);
        assertFalse(scheduler.recoverHeartbeatTask(client, claim, restart));
        verify(scheduler, never()).startTask(any());
    }

    @Test void userStopDuringRecoveryWins() throws Exception {
        when(client.stop()).thenAnswer(invocation -> { task.setStatus(TaskDto.STATUS_STOPPING); return true; });
        assertFalse(scheduler.recoverHeartbeatTask(client, claim, restart));
        verify(restart, never()).getAsBoolean();
        verify(scheduler, never()).startTask(any());
    }

    @Test void oldClientRequestCannotTouchReplacement() throws Exception {
        clients.put(task.getId().toHexString(), mock(TaskClient.class));
        assertFalse(scheduler.recoverHeartbeatTask(client, claim, restart));
        verify(claim, never()).getAsBoolean();
        verify(client, never()).stop();
    }

    @Test void genericRetryCannotBypassAnUnconfirmedWatchdogStop() {
        task.getAttrs().put("heartbeatRecovery", Map.of("state", "BLOCKED"));
        assertTrue(scheduler.heartbeatRecoveryOwnsRetry(client, null));
        assertTrue(scheduler.heartbeatRecoveryOwnsRetry(client, io.tapdata.flow.engine.V2.task.TerminalMode.ERROR));
        assertFalse(scheduler.heartbeatRecoveryOwnsRetry(client, io.tapdata.flow.engine.V2.task.TerminalMode.STOP_GRACEFUL));
    }

    @Test void asynchronousCancellationIsPolledUntilTerminalConfirmation() {
        when(client.stop()).thenReturn(false, true);
        doCallRealMethod().when(scheduler).stopHeartbeatTask(client);
        assertTrue(scheduler.stopHeartbeatTask(client));
        verify(client, times(2)).stop();
    }
}

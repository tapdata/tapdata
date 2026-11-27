package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRecoveryOnlyRunnerTest {
    @Test
    void replaysPausedTaskWithoutStartingNormalSourceAndClosesResourcesInReverseOrder() {
        List<String> emitted = new ArrayList<>();
        List<String> closed = new ArrayList<>();
        DqlReplaySourceNode replaySourceNode = new DqlReplaySourceNode() {
            @Override
            public void enqueue(TapdataDqlRecoveryEvent event) {
                emitted.add(event.getEventId());
            }

            @Override
            public void close() {
                closed.add("source");
            }
        };

        DqlRecoveryOnlyRunner runner = DqlRecoveryOnlyRunner.open(
                new DqlRecoveryOnlyRunner.TaskSnapshot("task-1", 7L, TaskDto.STATUS_STOP),
                replaySourceNode,
                closeable("graph", closed),
                closeable("context", closed)
        );

        runner.replay(List.of(
                recoveryEvent("event-1"),
                recoveryEvent("event-2")
        ));

        assertTrue(runner.taskSnapshot().isPaused());
        assertEquals(List.of("event-1", "event-2"), emitted);
        assertFalse(runner.normalSourceStarted());

        runner.close();
        assertEquals(List.of("source", "context", "graph"), closed);
    }

    private AutoCloseable closeable(String name, List<String> closed) {
        return () -> closed.add(name);
    }

    private TapdataDqlRecoveryEvent recoveryEvent(String eventId) {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", eventId));
        DqlPayloadSnapshot snapshot = new DqlPayloadSerializer().serialize(event);
        return TapdataDqlRecoveryEvent.createData(
                "batch-1", eventId, "attempt-1", "operator-1", 7L, snapshot);
    }
}

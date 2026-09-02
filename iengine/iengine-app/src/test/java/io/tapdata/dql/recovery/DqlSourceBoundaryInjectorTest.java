package io.tapdata.dql.recovery;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import com.tapdata.entity.TapdataDqlRecoveryEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DqlSourceBoundaryInjectorTest {

    @Test
    void injectsMultipleTablesThroughTheSingleDagSource() {
        DatabaseNode source = node("source");
        DAG dag = dagWithSources(source);
        List<String> tables = new ArrayList<>();
        DqlReplaySourceNode boundary = event -> tables.add(((io.tapdata.entity.event.dml.TapRecordEvent) event.getTapEvent()).getTableId());

        DqlSourceBoundaryInjector injector = new DqlSourceBoundaryInjector(
                dag,
                Map.of(source.getId(), boundary)
        );

        injector.enqueue(event("event-1", "orders"));
        injector.enqueue(event("event-2", "customers"));

        assertEquals(List.of("orders", "customers"), tables);
        assertEquals("source", injector.sourceNodeId());
    }

    @Test
    void rejectsRecoveryWhenDagSourceIsNotRegistered() {
        DatabaseNode source = node("source");
        DqlSourceBoundaryInjector injector = new DqlSourceBoundaryInjector(
                dagWithSources(source),
                Map.of()
        );

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> injector.enqueue(event("event-1", "orders"))
        );

        assertEquals("DLQ recovery source boundary is unavailable for DAG source node source", exception.getMessage());
    }

    @Test
    void doesNotAcceptDagTargetAsAnInjectionPoint() {
        DatabaseNode target = node("target");
        DAG dag = org.mockito.Mockito.mock(DAG.class);
        org.mockito.Mockito.when(dag.getSourceNodes()).thenReturn(List.of());
        org.mockito.Mockito.when(dag.getTargets()).thenReturn(List.of(target));

        DqlSourceBoundaryInjector injector = new DqlSourceBoundaryInjector(
                dag,
                Map.of(target.getId(), event -> {
                    throw new AssertionError("target must never receive recovery input");
                })
        );

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> injector.enqueue(event("event-1", "orders"))
        );

        assertEquals("DLQ recovery source boundary cannot be resolved from the task DAG", exception.getMessage());
    }

    @Test
    void rejectsAmbiguousMultipleSourceDagUntilTableRoutingIsExplicit() {
        DatabaseNode sourceA = node("source-a");
        DatabaseNode sourceB = node("source-b");
        DqlSourceBoundaryInjector injector = new DqlSourceBoundaryInjector(
                dagWithSources(sourceA, sourceB),
                Map.of(
                        sourceA.getId(), event -> {
                        },
                        sourceB.getId(), event -> {
                        }
                )
        );

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> injector.enqueue(event("event-1", "orders"))
        );

        assertEquals("DLQ recovery source boundary is ambiguous for DAG source nodes [source-a, source-b]", exception.getMessage());
    }

    private DatabaseNode node(String id) {
        DatabaseNode node = new DatabaseNode();
        node.setId(id);
        return node;
    }

    private DAG dagWithSources(Node<?>... nodes) {
        DAG dag = org.mockito.Mockito.mock(DAG.class);
        org.mockito.Mockito.when(dag.getSourceNodes()).thenReturn(List.of(nodes));
        return dag;
    }

    private TapdataDqlRecoveryEvent event(String eventId, String table) {
        TapInsertRecordEvent record = TapInsertRecordEvent.create()
                .table(table)
                .after(Map.of("id", eventId));
        DqlPayloadSnapshot snapshot = new DqlPayloadSerializer().serialize(record);
        return TapdataDqlRecoveryEvent.createData("batch-1", eventId, "attempt-1", "operator-1", 7L, snapshot);
    }
}

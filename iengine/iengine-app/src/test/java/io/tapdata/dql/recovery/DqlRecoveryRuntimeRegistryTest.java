package io.tapdata.dql.recovery;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.common.SettingService;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DqlRecoveryRuntimeRegistryTest {
    @Test
    void resolvesOnlyTheRegisteredSourceForTheCurrentTaskVersion() {
        DqlRecoveryRuntimeRegistry registry = new DqlRecoveryRuntimeRegistry();
        DatabaseNode source = new DatabaseNode();
        source.setId("source");
        DAG dag = mock(DAG.class);
        when(dag.getSourceNodes()).thenReturn(List.of(source));
        DqlReplaySourceNode boundary = event -> {
        };

        registry.register("task-1", 7L, dag, "source", boundary);

        DqlSourceBoundaryInjector resolved = registry.openSourceBoundary(command(7L));

        assertSame(boundary, resolved.sourceBoundary());
        assertEquals("source", resolved.sourceNodeId());
        assertTrue(registry.contains("task-1", 7L));
    }

    @Test
    void rejectsStaleTaskVersionsAndRemovesClosedSourceBoundaries() {
        DqlRecoveryRuntimeRegistry registry = new DqlRecoveryRuntimeRegistry();
        DatabaseNode source = new DatabaseNode();
        source.setId("source");
        DAG dag = mock(DAG.class);
        when(dag.getSourceNodes()).thenReturn(List.of(source));
        DqlReplaySourceNode boundary = event -> {
        };
        registry.register("task-1", 7L, dag, "source", boundary);

        assertThrows(IllegalStateException.class,
                () -> registry.openSourceBoundary(command(8L)));

        registry.unregister("task-1", 7L, "source", boundary);

        assertFalse(registry.contains("task-1", 7L));
        assertThrows(IllegalStateException.class,
                () -> registry.openSourceBoundary(command(7L)));
    }

    @Test
    void productionConfigurationPublishesTheRecoveryCoordinatorBean() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean("settingService", SettingService.class, () -> mock(SettingService.class));
            context.registerBean("clientMongoOperator", com.tapdata.mongo.ClientMongoOperator.class,
                    () -> new HttpClientMongoOperator(null, null, null, null));
            context.register(DqlRecoveryConfiguration.class);
            context.refresh();

            assertTrue(context.getBean(DqlRecoveryCoordinator.class)
                    instanceof DqlRecoveryCoordinatorImpl);
        }
    }

    private DqlRecoveryMessageDto command(long version) {
        DqlRecoveryMessageDto command = new DqlRecoveryMessageDto();
        command.setTaskId("task-1");
        command.setTaskVersion(version);
        return command;
    }
}

package io.tapdata.dql.recovery;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class HazelcastDqlRecoveryBatchRuntimeFactoryTest {

    @Test
    void restoresFormalTaskWhenStopFailsAfterSnapshotWasCaptured() {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setStatus(TaskDto.STATUS_RUNNING);
        DqlRecoveryTaskSnapshot snapshot = new DqlRecoveryTaskSnapshot(task, TaskDto.STATUS_RUNNING);

        DqlRecoveryTaskLifecycle lifecycle = mock(DqlRecoveryTaskLifecycle.class);
        when(lifecycle.stop(org.mockito.ArgumentMatchers.any(DqlRecoveryMessageDto.class)))
                .thenThrow(new DqlRecoveryTaskStopException(
                        "formal task stop failed", snapshot, new IllegalStateException("Jet stop failed")));
        HazelcastTaskService taskService = mock(HazelcastTaskService.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        HazelcastDqlRecoveryBatchRuntimeFactory factory =
                new HazelcastDqlRecoveryBatchRuntimeFactory(lifecycle, taskService, hazelcastInstance);

        assertThrows(DqlRecoveryTaskStopException.class, () -> factory.open(
                new DqlRecoveryMessageDto(),
                List.of(DqlRecoveryEvent.payloadOnly(new DqlPayloadSnapshot()))));

        verify(lifecycle).restore(snapshot);
        verifyNoInteractions(taskService, hazelcastInstance);
    }
}

package io.tapdata.dql.recovery;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRecoveryTaskSnapshotTest {

    @Test
    void capturesThePreRecoverySnapshotInterruptFlag() {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setSnapShotInterrupt(true);

        DqlRecoveryTaskSnapshot snapshot = new DqlRecoveryTaskSnapshot(task, TaskDto.STATUS_RUNNING);

        assertTrue(snapshot.snapshotInterruptBefore());
        task.setSnapShotInterrupt(false);
        assertTrue(snapshot.snapshotInterruptBefore());
    }

    @Test
    void defaultsMissingPreRecoverySnapshotInterruptFlagToFalse() {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());

        DqlRecoveryTaskSnapshot snapshot = new DqlRecoveryTaskSnapshot(task, TaskDto.STATUS_STOP);

        assertFalse(snapshot.snapshotInterruptBefore());
        assertTrue(snapshot.wasStopped());
    }

    @Test
    void leavesNonRunningTaskStateUntouchedDuringRestore() {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        TapdataTaskScheduler scheduler = new TapdataTaskScheduler();
        TapdataDqlRecoveryTaskLifecycle lifecycle = new TapdataDqlRecoveryTaskLifecycle(scheduler);

        lifecycle.restore(new DqlRecoveryTaskSnapshot(task, TaskDto.STATUS_WAIT_RUN));
    }
}

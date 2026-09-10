package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;

import java.util.Objects;

/** Adapter from DQL recovery to the formal Engine task scheduler. */
public final class TapdataDqlRecoveryTaskLifecycle implements DqlRecoveryTaskLifecycle {
    private final TapdataTaskScheduler scheduler;

    public TapdataDqlRecoveryTaskLifecycle(TapdataTaskScheduler scheduler) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    }

    @Override
    public DqlRecoveryTaskSnapshot stop(DqlRecoveryMessageDto command) {
        Objects.requireNonNull(command, "recovery command must not be null");
        return scheduler.stopTaskForDqlRecovery(command.getTaskId(), command.getTaskVersion());
    }

    @Override
    public void restore(DqlRecoveryTaskSnapshot snapshot) {
        scheduler.restoreTaskAfterDqlRecovery(snapshot);
    }
}

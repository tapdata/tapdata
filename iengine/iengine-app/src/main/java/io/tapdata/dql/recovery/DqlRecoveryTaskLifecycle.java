package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;

/** Formal task stop/restore boundary used by DQL recovery. */
public interface DqlRecoveryTaskLifecycle {
    DqlRecoveryTaskSnapshot stop(DqlRecoveryMessageDto command);

    void restore(DqlRecoveryTaskSnapshot snapshot);
}

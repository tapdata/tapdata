package io.tapdata.dql.recovery;

import java.util.List;
import io.tapdata.dql.model.DqlRecoveryNodeState;

/**
 * A temporary, isolated Jet runtime for one DQL recovery batch.  The runtime
 * owns task stop/restore, the replay source and the temporary job; the
 * coordinator only owns event ordering and reports.
 */
public interface DqlRecoveryBatchRuntime extends DqlRecoveryEventSink, AutoCloseable {
    DqlRecoveryBarrier barrier();

    default List<DqlRecoveryNodeState> nodeStates() {
        return List.of();
    }

    @Override
    void close();
}

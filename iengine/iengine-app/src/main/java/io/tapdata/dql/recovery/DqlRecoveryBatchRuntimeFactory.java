package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;

import java.util.List;

/** Opens a stopped-task, temporary DAG runtime for one recovery batch. */
@FunctionalInterface
public interface DqlRecoveryBatchRuntimeFactory {
    DqlRecoveryBatchRuntime open(DqlRecoveryMessageDto command, List<DqlRecoveryEvent> events);
}

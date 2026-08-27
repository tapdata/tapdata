package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;

/** Sends lifecycle callbacks for an accepted recovery batch. */
@FunctionalInterface
public interface DqlRecoveryReportSender {
    void reportBatchStarted(DqlRecoveryMessageDto command);
}

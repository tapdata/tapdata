package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.tm.commons.throttle.LogPositionStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors CDC log retention risk across source partitions relative to the earliest unconfirmed durable position.
 * Explicitly separates unknown dynamic retention and transient disconnects from confirmed log expiration.
 */
public class LogRetentionMonitor {

    public static final double WARN_RETENTION_RATIO = 0.70d;
    public static final double CRITICAL_RETENTION_RATIO = 0.90d;

    private final Map<String, PartitionAssessment> partitionStates = new ConcurrentHashMap<>();
    private volatile boolean stopAutoRetry;
    private volatile String expiredReason;
    private volatile String expiredPositionRef;

    /**
     * Evaluates a partition whose log retention window is known.
     */
    public PartitionAssessment assessKnownWindow(
            String partitionId,
            String earliestDurablePos,
            double usedRetentionRatio,
            long estimatedRemainingSeconds,
            long operatorBufferSeconds,
            double effectiveAckRate,
            double logProduceRate,
            long backlogSize) {
        String key = partitionId == null ? "default" : partitionId;
        double ratio = Math.max(0.0d, Math.min(1.0d, usedRetentionRatio));
        LogPositionStatus status = LogPositionStatus.NORMAL;
        String detail = "Log retention within normal window";

        boolean catchingUpImpossible = backlogSize > 0L && effectiveAckRate <= logProduceRate && logProduceRate > 0.0d;
        boolean belowOperatorBuffer = estimatedRemainingSeconds >= 0L && operatorBufferSeconds > 0L
                && estimatedRemainingSeconds < operatorBufferSeconds;

        if (ratio >= CRITICAL_RETENTION_RATIO || (belowOperatorBuffer && estimatedRemainingSeconds <= operatorBufferSeconds / 2L)) {
            status = LogPositionStatus.CRITICAL_90;
            detail = "Critical CDC log retention pressure (>=90% used or < half response buffer)";
        } else if (ratio >= WARN_RETENTION_RATIO || belowOperatorBuffer || catchingUpImpossible) {
            status = LogPositionStatus.WARN_70;
            detail = catchingUpImpossible
                    ? "CDC effective ack rate C <= log produce rate P with growing backlog"
                    : "CDC log retention approaching threshold (>=70% used or < response buffer)";
        }

        Double catchUpEtaSeconds = (backlogSize > 0L && effectiveAckRate > logProduceRate)
                ? (backlogSize / (effectiveAckRate - logProduceRate))
                : null;

        PartitionAssessment assessment = new PartitionAssessment(
                key, earliestDurablePos, status, ratio, estimatedRemainingSeconds, catchUpEtaSeconds, detail);
        partitionStates.put(key, assessment);
        return assessment;
    }

    /**
     * Records a partition whose source retention window is dynamic or cannot expose the oldest available offset.
     */
    public PartitionAssessment markUnknownWindow(String partitionId, String earliestDurablePos, long cdcLagSeconds, long staleCheckpointSeconds) {
        String key = partitionId == null ? "default" : partitionId;
        String detail = "Log retention margin unknown (lag=" + cdcLagSeconds + "s, checkpointStale=" + staleCheckpointSeconds + "s)";
        PartitionAssessment assessment = new PartitionAssessment(
                key, earliestDurablePos, LogPositionStatus.UNKNOWN, -1.0d, -1L, null, detail);
        partitionStates.put(key, assessment);
        return assessment;
    }

    /**
     * Handles a source CDC read failure conservatively:
     * - Transient network/permission/query failures stay UNKNOWN and permit normal retry.
     * - Only confirmed source position expiration sets EXPIRED and locks automatic retry / snapshot fallback / jump-to-latest.
     */
    public PartitionAssessment handleReadFailure(
            String partitionId,
            String earliestDurablePos,
            boolean confirmedPositionExpired,
            String reason) {
        String key = partitionId == null ? "default" : partitionId;
        if (!confirmedPositionExpired) {
            PartitionAssessment transientState = new PartitionAssessment(
                    key,
                    earliestDurablePos,
                    LogPositionStatus.UNKNOWN,
                    -1.0d,
                    -1L,
                    null,
                    reason == null ? "Transient CDC read error; pending verification" : reason);
            partitionStates.put(key, transientState);
            return transientState;
        }
        String finalReason = reason == null || reason.isBlank() ? "CDC incremental log position expired" : reason;
        this.stopAutoRetry = true;
        this.expiredReason = finalReason;
        this.expiredPositionRef = earliestDurablePos;

        PartitionAssessment expired = new PartitionAssessment(
                key,
                earliestDurablePos,
                LogPositionStatus.EXPIRED,
                1.0d,
                0L,
                null,
                finalReason);
        partitionStates.put(key, expired);
        return expired;
    }

    /**
     * Computes task-level worst status across all observed partitions.
     */
    public LogPositionStatus worstStatus() {
        if (stopAutoRetry) {
            return LogPositionStatus.EXPIRED;
        }
        LogPositionStatus worst = LogPositionStatus.NORMAL;
        for (PartitionAssessment p : partitionStates.values()) {
            if (severityRank(p.status()) > severityRank(worst)) {
                worst = p.status();
            }
        }
        return worst;
    }

    public boolean shouldStopAutoRetry() {
        return stopAutoRetry;
    }

    public String getExpiredReason() {
        return expiredReason;
    }

    public String getExpiredPositionRef() {
        return expiredPositionRef;
    }

    public Map<String, PartitionAssessment> getPartitionStates() {
        return Map.copyOf(partitionStates);
    }

    private static int severityRank(LogPositionStatus status) {
        if (status == null) {
            return 0;
        }
        return switch (status) {
            case NORMAL -> 0;
            case UNKNOWN -> 1;
            case WARN_70 -> 2;
            case CRITICAL_90 -> 3;
            case EXPIRED -> 4;
        };
    }

    public record PartitionAssessment(
            String partitionId,
            String earliestDurablePos,
            LogPositionStatus status,
            double usedRatio,
            long estimatedRemainingSeconds,
            Double estimatedCatchUpSeconds,
            String detail
    ) {
    }
}

package io.tapdata.flow.engine.V2.sharecdc;

/**
 * Listener notified by enterprise share-CDC readers when ringbuffer retention windows
 * or sequence expiration boundaries are observed for a throttled consumer task.
 */
public interface ShareCdcRetentionListener {

    void onWindowObserved(String tableName, long currentSequence, long headSequence, long tailSequence, long eventTimestampMs, int ttlDays);

    void onPositionExpired(String tableName, long failedSequence, String reason);

    void onTransientReadFailure(String tableName, long currentSequence, String reason);
}

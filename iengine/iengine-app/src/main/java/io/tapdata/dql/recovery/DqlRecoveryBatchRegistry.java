package io.tapdata.dql.recovery;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-local ownership registry for recovery batches.
 *
 * <p>The websocket framework creates event handlers per message, so the
 * production registry must outlive an individual handler instance.</p>
 */
public final class DqlRecoveryBatchRegistry {
    private static final DqlRecoveryBatchRegistry GLOBAL = new DqlRecoveryBatchRegistry();

    private final Set<String> claimedBatchIds = ConcurrentHashMap.newKeySet();

    public static DqlRecoveryBatchRegistry global() {
        return GLOBAL;
    }

    public boolean claim(String batchId) {
        return claimedBatchIds.add(batchId);
    }

    public void release(String batchId) {
        claimedBatchIds.remove(batchId);
    }
}

package io.tapdata.dql.recovery;

import com.hazelcast.core.HazelcastInstance;

import java.util.concurrent.TimeUnit;

/** Distributed completion marker for barrier events crossing a Hazelcast queue. */
public final class DqlRecoveryBarrierSignalStore {
    private static final String MAP_NAME = "dql-recovery-barrier-signals";
    private static final long SIGNAL_TTL_MINUTES = 10L;

    private DqlRecoveryBarrierSignalStore() {
    }

    public static void signal(HazelcastInstance hazelcastInstance, String barrierId) {
        if (hazelcastInstance != null && barrierId != null && !barrierId.isBlank()) {
            hazelcastInstance.<String, Boolean>getMap(MAP_NAME)
                    .put(barrierId, Boolean.TRUE, SIGNAL_TTL_MINUTES, TimeUnit.MINUTES);
        }
    }

    public static boolean isSignaled(HazelcastInstance hazelcastInstance, String barrierId) {
        return hazelcastInstance != null
                && barrierId != null
                && hazelcastInstance.<String, Boolean>getMap(MAP_NAME).containsKey(barrierId);
    }

    public static void remove(HazelcastInstance hazelcastInstance, String barrierId) {
        if (hazelcastInstance != null && barrierId != null && !barrierId.isBlank()) {
            hazelcastInstance.<String, Boolean>getMap(MAP_NAME).remove(barrierId);
        }
    }
}

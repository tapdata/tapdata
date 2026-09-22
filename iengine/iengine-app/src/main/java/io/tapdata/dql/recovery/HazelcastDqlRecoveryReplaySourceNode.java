package io.tapdata.dql.recovery;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.entity.TapdataCountDownLatchEvent;
import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.entity.TapdataEvent;

import java.util.Objects;

/** Engine-side producer for the temporary DQL replay source queue. */
public final class HazelcastDqlRecoveryReplaySourceNode implements DqlReplaySourceNode {
    private final IQueue<TapdataEvent> queue;
    private final String queueName;

    public HazelcastDqlRecoveryReplaySourceNode(HazelcastInstance hazelcastInstance, String queueName) {
        Objects.requireNonNull(hazelcastInstance, "hazelcastInstance must not be null");
        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("recovery queue name must not be blank");
        }
        this.queueName = queueName;
        this.queue = hazelcastInstance.getQueue(queueName);
    }

    @Override
    public void enqueue(TapdataDqlRecoveryEvent event) {
        enqueueEvent(event);
    }

    @Override
    public void enqueueBarrier(TapdataCountDownLatchEvent event) {
        enqueueEvent(event);
    }

    private void enqueueEvent(TapdataEvent event) {
        Objects.requireNonNull(event, "recovery event must not be null");
        try {
            queue.put(event);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while enqueueing DQL recovery event", exception);
        }
    }

    @Override
    public void close() {
        queue.destroy();
    }
}

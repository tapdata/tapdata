package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataCountDownLatchEvent;
import com.tapdata.entity.TapdataDqlRecoveryEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkBaseNode;

import java.util.Objects;

/** Adapts a live PDK source node to the DQL recovery source-boundary contract. */
public final class HazelcastDqlReplaySourceNode implements DqlReplaySourceNode {
    private final HazelcastSourcePdkBaseNode sourceNode;

    public HazelcastDqlReplaySourceNode(HazelcastSourcePdkBaseNode sourceNode) {
        this.sourceNode = Objects.requireNonNull(sourceNode, "source node must not be null");
    }

    @Override
    public void enqueue(TapdataDqlRecoveryEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("DLQ recovery event must not be null");
        }
        sourceNode.enqueue(event);
    }

    @Override
    public void enqueueBarrier(TapdataCountDownLatchEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("DLQ recovery barrier event must not be null");
        }
        sourceNode.enqueue(event);
    }

    @Override
    public void prepareForRecovery(long timeoutMillis) throws InterruptedException {
        sourceNode.getDqlSourceReadGate().prepareForRecovery(timeoutMillis);
    }

    @Override
    public void restoreAfterRecovery() {
        sourceNode.getDqlSourceReadGate().restoreAfterRecovery();
    }
}

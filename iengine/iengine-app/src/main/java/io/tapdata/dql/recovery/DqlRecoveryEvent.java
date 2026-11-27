package io.tapdata.dql.recovery;

import io.tapdata.dql.model.DqlPayloadSnapshot;

import java.util.Objects;

/**
 * Immutable Engine view of one DQL event.  The payload alone is not enough to
 * build a safe replay graph; the failed node and the source/target metadata
 * are part of the recovery contract as well.
 */
public record DqlRecoveryEvent(DqlPayloadSnapshot payload,
                               String sourceNodeId,
                               String sourceNodeName,
                               String failedNodeId,
                               String failedNodeName,
                               String targetNodeId,
                               String targetNodeName) {

    public DqlRecoveryEvent {
        Objects.requireNonNull(payload, "payload must not be null");
    }

    public static DqlRecoveryEvent payloadOnly(DqlPayloadSnapshot payload) {
        return new DqlRecoveryEvent(payload, null, null, null, null, null, null);
    }
}

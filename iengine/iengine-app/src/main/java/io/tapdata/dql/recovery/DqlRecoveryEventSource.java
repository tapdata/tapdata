package io.tapdata.dql.recovery;

import io.tapdata.dql.model.DqlPayloadSnapshot;

/** Loads the immutable payload snapshot for one TM DQL event. */
@FunctionalInterface
public interface DqlRecoveryEventSource {
    DqlPayloadSnapshot load(String eventId);

    /**
     * Loads the payload and the DAG node metadata needed for from-failed-node
     * replay.  The default keeps older test doubles and old Engine plugins
     * source-compatible; production TM adapters override it.
     */
    default DqlRecoveryEvent loadEvent(String eventId) {
        return DqlRecoveryEvent.payloadOnly(load(eventId));
    }
}

package io.tapdata.dql.recovery;

import io.tapdata.dql.model.DqlPayloadSnapshot;

/** Loads the immutable payload snapshot for one TM DQL event. */
@FunctionalInterface
public interface DqlRecoveryEventSource {
    DqlPayloadSnapshot load(String eventId);
}

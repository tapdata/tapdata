package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;

/** Injects one recovery event at the task's source boundary. */
@FunctionalInterface
public interface DqlRecoveryEventSink {
    void enqueue(TapdataDqlRecoveryEvent event);
}

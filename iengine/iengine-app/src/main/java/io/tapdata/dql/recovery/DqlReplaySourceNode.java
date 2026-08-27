package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;

/**
 * Recovery-only source boundary. It exposes replay injection only and has no
 * API that can start a connector or a normal source reader.
 */
public interface DqlReplaySourceNode extends AutoCloseable {
    void enqueue(TapdataDqlRecoveryEvent event);

    @Override
    default void close() {
    }
}

package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataCountDownLatchEvent;
import com.tapdata.entity.TapdataDqlRecoveryEvent;

/**
 * Recovery-only source boundary. It exposes replay injection only and has no
 * API that can start a connector or a normal source reader.
 */
public interface DqlReplaySourceNode extends AutoCloseable {
    void enqueue(TapdataDqlRecoveryEvent event);

    /**
     * Sends the queue barrier behind a recovery DATA event. A concrete source
     * must override this when it supports end-to-end target completion.
     */
    default void enqueueBarrier(TapdataCountDownLatchEvent event) {
        throw new UnsupportedOperationException("source boundary does not support recovery barriers");
    }

    @Override
    default void close() {
    }
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import java.util.concurrent.CancellationException;
import io.tapdata.pdk.core.utils.CommonUtils;

/** A stopped exactly-once batch was rolled back and must not advance its offset. */
public class ExactlyOnceWriteCancelledException extends CancellationException {
    public static boolean isCleanCancellation(Throwable error) {
        Throwable cancelled = CommonUtils.matchThrowable(error, ExactlyOnceWriteCancelledException.class);
        return cancelled != null && cancelled.getSuppressed().length == 0;
    }

    public ExactlyOnceWriteCancelledException() {
        super("Node stopped during exactly once write; rolling back the batch");
    }
}

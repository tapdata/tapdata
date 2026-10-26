package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import java.util.concurrent.CancellationException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** A stopped exactly-once batch was rolled back and must not advance its offset. */
public class ExactlyOnceWriteCancelledException extends CancellationException {
    public static boolean isCleanCancellation(Throwable error) {
        // Stop interrupts the worker; matching must still traverse PDK wrappers without clearing that flag.
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        while (error != null && visited.add(error)) {
            if (error instanceof ExactlyOnceWriteCancelledException) {
                return error.getSuppressed().length == 0;
            }
            error = error.getCause();
        }
        return false;
    }

    public ExactlyOnceWriteCancelledException() {
        super("Node stopped during exactly once write; rolling back the batch");
    }
}

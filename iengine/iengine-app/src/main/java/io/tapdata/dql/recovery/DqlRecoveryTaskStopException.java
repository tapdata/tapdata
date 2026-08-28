package io.tapdata.dql.recovery;

/**
 * Signals that DQL recovery could not finish stopping the formal task after
 * its pre-recovery state had already been captured.
 *
 * <p>The snapshot is deliberately carried by the exception.  Stopping a Jet
 * job and updating TM are asynchronous operations, so the lifecycle boundary
 * may fail after the caller has lost the normal return value.  Carrying the
 * snapshot lets the recovery runtime execute the same compensation path for
 * both stop failures and replay failures.</p>
 */
public final class DqlRecoveryTaskStopException extends IllegalStateException {
    private final DqlRecoveryTaskSnapshot snapshot;

    public DqlRecoveryTaskStopException(String message,
                                        DqlRecoveryTaskSnapshot snapshot,
                                        Throwable cause) {
        super(message, cause);
        this.snapshot = snapshot;
    }

    public DqlRecoveryTaskSnapshot snapshot() {
        return snapshot;
    }
}

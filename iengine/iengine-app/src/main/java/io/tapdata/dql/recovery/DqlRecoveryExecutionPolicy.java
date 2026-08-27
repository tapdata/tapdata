package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.config.DqlRuntimeConfig;

/** Decides whether a batch should move to the next event after a failure. */
@FunctionalInterface
public interface DqlRecoveryExecutionPolicy {
    boolean continueAfterFailure();

    static DqlRecoveryExecutionPolicy from(DqlRuntimeConfig config) {
        DqlRuntimeConfig effective = config == null ? DqlRuntimeConfig.defaults() : config;
        return effective::isRecoveryContinueOnEventFailure;
    }
}

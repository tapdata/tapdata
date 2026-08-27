package com.tapdata.tm.dql;

/**
 * Result of an idempotent recovery callback transition.
 */
public enum DqlRecoveryCallbackResultEnum {
    APPLIED,
    DUPLICATE,
    NOT_IN_BATCH,
    CONFLICT
}

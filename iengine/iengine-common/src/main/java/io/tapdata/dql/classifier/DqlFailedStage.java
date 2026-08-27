package io.tapdata.dql.classifier;

/**
 * Engine stage where a DQL candidate failure was observed.
 */
public enum DqlFailedStage {
    SOURCE_READ,
    PROCESSOR,
    TARGET_WRITE,
    TM_CALLBACK,
    OTHER
}

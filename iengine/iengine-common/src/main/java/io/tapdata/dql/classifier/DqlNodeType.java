package io.tapdata.dql.classifier;

/**
 * Node category associated with a DLQ candidate failure.
 */
public enum DqlNodeType {
    SOURCE,
    PROCESSOR,
    TARGET,
    CONNECTOR,
    TM_CALLBACK,
    OTHER
}

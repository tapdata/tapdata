package io.tapdata.dql.classifier;

/**
 * Node category associated with a DQL candidate failure.
 */
public enum DqlNodeType {
    SOURCE,
    PROCESSOR,
    TARGET,
    CONNECTOR,
    TM_CALLBACK,
    OTHER
}

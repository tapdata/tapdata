package io.tapdata.pdk.apis;

import io.tapdata.pdk.apis.context.TapConnectionContext;

/**
 * Tapdata node in a DAG
 */
public interface TapNode {
    /**
     * Tapdata node closed in a DAG
     */
//    void destroy(TapConnectionContext connectionContext) throws Throwable;

    void init(TapConnectionContext connectionContext) throws Throwable;

    void stop(TapConnectionContext connectionContext) throws Throwable;
}

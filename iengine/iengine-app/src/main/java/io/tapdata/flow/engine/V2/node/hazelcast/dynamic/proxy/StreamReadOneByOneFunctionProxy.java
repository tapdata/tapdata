package io.tapdata.flow.engine.V2.node.hazelcast.dynamic.proxy;

import io.tapdata.pdk.apis.functions.connector.source.StreamReadOneByOneFunction;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/9 10:45 Create
 * @description
 */
public class StreamReadOneByOneFunctionProxy extends StreamReadNormalBaseProxy<StreamReadOneByOneFunction> {

    public StreamReadOneByOneFunctionProxy(StreamReadOneByOneFunction function) {
        super(function);
    }
}

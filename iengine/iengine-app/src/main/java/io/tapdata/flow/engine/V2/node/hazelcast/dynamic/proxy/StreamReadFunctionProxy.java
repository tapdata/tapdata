package io.tapdata.flow.engine.V2.node.hazelcast.dynamic.proxy;

import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/9 11:11 Create
 * @description
 */
public class StreamReadFunctionProxy extends StreamReadNormalBaseProxy<StreamReadFunction> {
    public StreamReadFunctionProxy(StreamReadFunction function) {
        super(function);
    }
}

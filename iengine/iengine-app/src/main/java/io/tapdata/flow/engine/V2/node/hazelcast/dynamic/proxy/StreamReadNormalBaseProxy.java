package io.tapdata.flow.engine.V2.node.hazelcast.dynamic.proxy;

import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/9 11:24 Create
 * @description
 */
public class StreamReadNormalBaseProxy<T extends TapFunction> extends StreamReadBaseProxy<T, String> {
    protected StreamReadNormalBaseProxy(T function) {
        super(function);
    }

    @Override
    protected boolean skip(List<String> tables, String tableName) {
        return !tables.contains(tableName);
    }

    @Override
    protected void remove(List<String> tables, String tableName) {
        tables.remove(tableName);
    }
}

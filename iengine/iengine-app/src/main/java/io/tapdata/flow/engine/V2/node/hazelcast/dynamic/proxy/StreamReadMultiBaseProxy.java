package io.tapdata.flow.engine.V2.node.hazelcast.dynamic.proxy;

import io.tapdata.pdk.apis.functions.connector.TapFunction;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/9 11:21 Create
 * @description
 */
public class StreamReadMultiBaseProxy<T extends TapFunction> extends StreamReadBaseProxy<T, ConnectionConfigWithTables> {

    protected StreamReadMultiBaseProxy(T function) {
        super(function);
    }

    @Override
    protected void remove(List<ConnectionConfigWithTables> tables, String tableName) {
        for (int i = tables.size() - 1; i >= 0; i--) {
            ConnectionConfigWithTables connectionConfigWithTables = tables.get(i);
            List<String> tableList = connectionConfigWithTables.getTables();
            if (null != tableList) {
                tableList.remove(tableName);
            }
        }
    }
}

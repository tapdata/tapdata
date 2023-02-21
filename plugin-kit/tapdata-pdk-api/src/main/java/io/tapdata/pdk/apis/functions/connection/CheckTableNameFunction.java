package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.function.Consumer;

public interface CheckTableNameFunction extends TapFunction {
    /**
     * @param nodeContext the node context in a DAG
     */
    CheckTableNameResult check(TapConnectionContext nodeContext, String tableName) throws Throwable;
}

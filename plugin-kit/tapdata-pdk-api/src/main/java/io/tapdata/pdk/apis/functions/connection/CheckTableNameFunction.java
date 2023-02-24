package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectionFunction;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.function.Consumer;

/**
 * Check whether the table name is legal for the data source.
 */
public interface CheckTableNameFunction extends TapConnectionFunction {
    /**
     * @param nodeContext the node context in a DAG
     */
    CheckTableNameResult check(TapConnectionContext nodeContext, String tableName) throws Throwable;
}

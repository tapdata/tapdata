package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectionFunction;

/**
 * Get table information, like number of rows, storage size, etc.
 */
public interface GetTableInfoFunction extends TapConnectionFunction {
    TableInfo getTableInfo(TapConnectionContext connectionContext, String tableName) throws Throwable;
}

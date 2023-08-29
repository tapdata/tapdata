package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

public interface CountRawCommandFunction extends TapConnectorFunction {

    long count(TapConnectorContext connectorContext, String command, TapTable table) throws Throwable;
}



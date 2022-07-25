package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterTableCharsetEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface AlterTableCharsetFunction extends TapConnectorFunction {
    void alterTableCharset(TapConnectorContext connectorContext, TapAlterTableCharsetEvent alterTableCharsetEvent) throws Throwable;
}

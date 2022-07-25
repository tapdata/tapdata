package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterDatabaseTimezoneEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface AlterDatabaseTimeZoneFunction extends TapConnectorFunction {
    void alterDatabaseTimeZone(TapConnectorContext connectorContext, TapAlterDatabaseTimezoneEvent alterDatabaseTimeZoneEvent) throws Throwable;
}

package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterDatabaseTimeZoneEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface AlterDatabaseTimeZoneFunction extends TapFunction {
    void alterDatabaseTimeZone(TapConnectorContext connectorContext, TapAlterDatabaseTimeZoneEvent alterDatabaseTimeZoneEvent) throws Throwable;
}

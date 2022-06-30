package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterDatabaseTimeZoneEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldConstraintEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface AlterDatabaseTimeZoneFunction {
    void alterDatabaseTimeZone(TapConnectorContext connectorContext, TapAlterDatabaseTimeZoneEvent alterDatabaseTimeZoneEvent) throws Throwable;
}

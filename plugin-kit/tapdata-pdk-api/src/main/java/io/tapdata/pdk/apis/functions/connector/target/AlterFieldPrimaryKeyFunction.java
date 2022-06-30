package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldCheckEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldPrimaryKeyEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface AlterFieldPrimaryKeyFunction {
    void alterFieldPrimaryKey(TapConnectorContext connectorContext, TapAlterFieldPrimaryKeyEvent alterFieldPrimaryKeyEvent) throws Throwable;
}

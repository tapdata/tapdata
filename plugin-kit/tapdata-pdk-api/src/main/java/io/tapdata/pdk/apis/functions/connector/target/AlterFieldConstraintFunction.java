package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldConstraintEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldPrimaryKeyEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface AlterFieldConstraintFunction {
    void alterFieldConstraint(TapConnectorContext connectorContext, TapAlterFieldConstraintEvent alterFieldConstraintEvent) throws Throwable;
}

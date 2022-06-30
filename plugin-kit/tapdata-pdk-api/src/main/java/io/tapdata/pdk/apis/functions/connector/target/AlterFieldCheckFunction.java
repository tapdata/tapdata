package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldCheckEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface AlterFieldCheckFunction {
    void alterFieldCheck(TapConnectorContext connectorContext, TapAlterFieldCheckEvent alterFieldCheckEvent) throws Throwable;
}

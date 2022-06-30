package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNotNullEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface AlterFieldNotNullFunction {
    void alterNotNull(TapConnectorContext connectorContext, TapAlterFieldNotNullEvent alterFieldNotNullEvent) throws Throwable;
}

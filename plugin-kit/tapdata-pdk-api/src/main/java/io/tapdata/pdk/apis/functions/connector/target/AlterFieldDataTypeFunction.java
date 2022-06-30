package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldDataTypeEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface AlterFieldDataTypeFunction {
    void alterFieldDataType(TapConnectorContext connectorContext, TapAlterFieldDataTypeEvent alterFieldDataTypeEvent) throws Throwable;
}

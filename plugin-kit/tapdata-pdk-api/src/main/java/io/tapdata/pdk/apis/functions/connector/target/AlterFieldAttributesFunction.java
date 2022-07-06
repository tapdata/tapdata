package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface AlterFieldAttributesFunction {
    void alterFieldDataType(TapConnectorContext connectorContext, TapAlterFieldAttributesEvent alterFieldDataTypeEvent) throws Throwable;
}

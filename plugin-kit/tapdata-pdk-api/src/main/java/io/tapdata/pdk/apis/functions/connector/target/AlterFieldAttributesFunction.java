package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface AlterFieldAttributesFunction extends TapConnectorFunction {
    void alterFieldAttributes(TapConnectorContext connectorContext, TapAlterFieldAttributesEvent alterFieldDataTypeEvent) throws Throwable;
}

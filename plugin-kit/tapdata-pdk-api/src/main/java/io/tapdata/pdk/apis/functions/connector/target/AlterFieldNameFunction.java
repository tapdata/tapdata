package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface AlterFieldNameFunction extends TapConnectorFunction {
    void alterFieldName(TapConnectorContext connectorContext, TapAlterFieldNameEvent alterFieldNameEvent) throws Throwable;
}

package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldDefaultEvent;
import io.tapdata.entity.event.ddl.table.TapAlterTableCharsetEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface AlterFieldDefaultFunction {
    void alterFieldDefault(TapConnectorContext connectorContext, TapAlterFieldDefaultEvent alterFieldDefaultEvent) throws Throwable;
}

package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;

public interface QueryFieldMinMaxValueFunction extends TapConnectorFunction {
    FieldMinMaxValue minMaxValue(TapConnectorContext connectorContext, TapTable table, TapAdvanceFilter filter, String fieldName);
}
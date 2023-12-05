package io.tapdata.MockConnector;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class MockAdvanceFilterQueryFunction  implements QueryByAdvanceFilterFunction {


    private TapAdvanceFilter tapAdvanceFilter;
    @Override
    public void query(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) throws Throwable {
        this.tapAdvanceFilter = tapAdvanceFilter;
    }

    public TapAdvanceFilter getTapAdvanceFilter() {
        return tapAdvanceFilter;
    }

    public void setTapAdvanceFilter(TapAdvanceFilter tapAdvanceFilter) {
        this.tapAdvanceFilter = tapAdvanceFilter;
    }
}

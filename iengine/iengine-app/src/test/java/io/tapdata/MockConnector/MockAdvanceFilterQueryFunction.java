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
    protected Object data;
    private boolean flag;
    private TapAdvanceFilter tapAdvanceFilter;
    @Override
    public void query(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) throws Throwable {
        this.flag = true;
        this.tapAdvanceFilter = tapAdvanceFilter;
        FilterResults filterResults = new FilterResults();
        if(data instanceof Exception) {
            filterResults.setError((Throwable) data);
        }else {
            filterResults.setResults((List<Map<String, Object>>)data);
        }
        consumer.accept(filterResults);
    }

    public TapAdvanceFilter getTapAdvanceFilter() {
        return tapAdvanceFilter;
    }

    public void setTapAdvanceFilter(TapAdvanceFilter tapAdvanceFilter) {
        this.tapAdvanceFilter = tapAdvanceFilter;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}

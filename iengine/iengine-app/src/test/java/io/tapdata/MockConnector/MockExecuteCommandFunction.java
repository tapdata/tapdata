package io.tapdata.MockConnector;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;

import java.util.function.Consumer;

public class MockExecuteCommandFunction implements ExecuteCommandFunction {

    protected Object data;


    @Override
    public void execute(TapConnectorContext tapConnectorContext, TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> consumer) throws Throwable {
    }

    public void setData(Object data) {
        this.data = data;
    }
}

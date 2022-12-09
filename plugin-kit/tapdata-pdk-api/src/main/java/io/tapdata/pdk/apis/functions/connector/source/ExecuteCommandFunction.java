package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

import java.util.function.Consumer;

public interface ExecuteCommandFunction extends TapConnectorFunction {
    /**
     * @param connectorContext the node context in a DAG
     */
    void execute(TapConnectorContext connectorContext, TapExecuteCommand executeCommand, Consumer<ExecuteResult> consumer) throws Throwable;
}

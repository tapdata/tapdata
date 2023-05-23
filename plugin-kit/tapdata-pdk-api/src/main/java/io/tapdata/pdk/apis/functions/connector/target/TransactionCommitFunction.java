package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

import java.util.List;
import java.util.function.Consumer;

public interface TransactionCommitFunction extends TapConnectorFunction {

    void commit(TapConnectorContext connectorContext) throws Throwable;

}

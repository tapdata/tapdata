package io.tapdata.pdk.apis.functions;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.connection.*;
import io.tapdata.pdk.apis.functions.connector.TapFunction;
import io.tapdata.pdk.apis.functions.connector.common.ReleaseExternalFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.apis.functions.connector.target.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ConnectionFunctions<T extends ConnectionFunctions<?>> extends CommonFunctions<T> {
    protected GetTableInfoFunction getTableInfoFunction;
    protected CheckTableNameFunction checkTableNameFunction;
    protected GetTableNamesFunction getTableNamesFunction;
    protected ConnectionCheckFunction connectionCheckFunction;
    protected GetCharsetsFunction getCharsetsFunction;
    protected CommandCallbackFunction commandCallbackFunction;
    protected ErrorHandleFunction errorHandleFunction;
    public T supportGetTableInfoFunction(GetTableInfoFunction function) {
        getTableInfoFunction = function;
        return (T) this;
    }

    public T supportCheckTableNameFunction(CheckTableNameFunction function) {
        checkTableNameFunction = function;
        return (T) this;
    }
    public T supportCommandCallbackFunction(CommandCallbackFunction function) {
        commandCallbackFunction = function;
        return (T) this;
    }
    public T supportGetCharsetsFunction(GetCharsetsFunction function) {
        getCharsetsFunction = function;
        return (T) this;
    }
    public T supportGetTableNamesFunction(GetTableNamesFunction function) {
        getTableNamesFunction = function;
        return (T) this;
    }
    public T supportConnectionCheckFunction(ConnectionCheckFunction function) {
        connectionCheckFunction = function;
        return (T) this;
    }
    public T supportErrorHandleFunction(ErrorHandleFunction function) {
        errorHandleFunction = function;
        return (T) this;
    }

    public GetTableNamesFunction getGetTableNamesFunction() {
        return getTableNamesFunction;
    }

    public ConnectionCheckFunction getConnectionCheckFunction() {
        return connectionCheckFunction;
    }

    public GetCharsetsFunction getGetCharsetsFunction() {
        return getCharsetsFunction;
    }
    public CommandCallbackFunction getCommandCallbackFunction() {
        return commandCallbackFunction;
    }
    public ErrorHandleFunction getErrorHandleFunction() {
        return errorHandleFunction;
    }

    public CheckTableNameFunction getCheckTableNameFunction() {
        return checkTableNameFunction;
    }

    public GetTableInfoFunction getGetTableInfoFunction() {
        return getTableInfoFunction;
    }
}

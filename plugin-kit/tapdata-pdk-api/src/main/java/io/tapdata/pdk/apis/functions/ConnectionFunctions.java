package io.tapdata.pdk.apis.functions;

import io.tapdata.pdk.apis.functions.connection.CheckTableNameFunction;
import io.tapdata.pdk.apis.functions.connection.CommandCallbackFunction;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckFunction;
import io.tapdata.pdk.apis.functions.connection.ConnectorWebsiteFunction;
import io.tapdata.pdk.apis.functions.connection.ErrorHandleFunction;
import io.tapdata.pdk.apis.functions.connection.GetCharsetsFunction;
import io.tapdata.pdk.apis.functions.connection.GetTableInfoFunction;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.apis.functions.connection.TableWebsiteFunction;

public class ConnectionFunctions<T extends ConnectionFunctions<?>> extends CommonFunctions<T> {
    protected GetTableInfoFunction getTableInfoFunction;
    protected CheckTableNameFunction checkTableNameFunction;
    protected GetTableNamesFunction getTableNamesFunction;
    protected ConnectionCheckFunction connectionCheckFunction;
    protected GetCharsetsFunction getCharsetsFunction;
    protected CommandCallbackFunction commandCallbackFunction;
    protected ErrorHandleFunction errorHandleFunction;


    protected ConnectorWebsiteFunction connectorWebsiteFunction;
    protected TableWebsiteFunction tableWebsiteFunction;

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

    public ConnectionFunctions supportConnectorWebsite(ConnectorWebsiteFunction connectorWebsiteFunction){
        this.connectorWebsiteFunction = connectorWebsiteFunction;
        return this;
    }

    public ConnectionFunctions supportTableWebsite(TableWebsiteFunction tableWebsiteFunction){
        this.tableWebsiteFunction = tableWebsiteFunction;
        return this;
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

    public TableWebsiteFunction getTableWebsiteFunction(){
        return this.tableWebsiteFunction;
    }

    public ConnectorWebsiteFunction getConnectorWebsiteFunction(){
        return this.connectorWebsiteFunction;
    }
}

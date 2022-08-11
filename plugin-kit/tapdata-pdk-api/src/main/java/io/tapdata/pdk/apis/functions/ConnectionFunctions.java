package io.tapdata.pdk.apis.functions;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckFunction;
import io.tapdata.pdk.apis.functions.connection.GetCharsetsFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
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
    protected GetTableNamesFunction getTableNamesFunction;
    protected ConnectionCheckFunction connectionCheckFunction;
    protected GetCharsetsFunction getCharsetsFunction;

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

    public GetTableNamesFunction getGetTableNamesFunction() {
        return getTableNamesFunction;
    }

    public ConnectionCheckFunction getConnectionCheckFunction() {
        return connectionCheckFunction;
    }

    public GetCharsetsFunction getGetCharsetsFunction() {
        return getCharsetsFunction;
    }
}

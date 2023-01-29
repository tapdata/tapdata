package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;

public class JSCreateTableV2Function extends FunctionBase implements FunctionSupport<CreateTableV2Function> {
    private JSCreateTableV2Function() {
        super();
        super.functionName = JSFunctionNames.CreateTableV2Function;
    }

    @Override
    public CreateTableV2Function function(LoadJavaScripter javaScripter) {
        //if (super.hasNotSupport(javaScripter)) return null;
        return null;//this::createTableV2;
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        return null;
    }

    public static CreateTableV2Function create(LoadJavaScripter loadJavaScripter) {
        return new JSCreateTableV2Function().function(loadJavaScripter);
    }
}

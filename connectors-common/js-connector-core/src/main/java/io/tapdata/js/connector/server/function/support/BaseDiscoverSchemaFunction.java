package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.js.connector.server.function.base.SchemaAccept;
import io.tapdata.js.connector.server.function.base.SchemaSender;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
//Table Structure Statement:
//(1)Only one table name:
/**
 * "TableName";
 * <p>
 * ["Table1","Table2"];
 * <p>
 * ["Table1","Table2"];
 * <p>
 * ["Table1","Table2"];
 */
//(2)Return table list only:
/**
 * ["Table1","Table2"];
 * */
//(3)Return detailed table information:
// [{},{},['','',''],{}],list of tables , every item as a table info. if each item be list, the list must be table name(String) list.
//The field has the following properties: type  |  default  |  nullable  |  isPrimaryKey  |  autoInc | comment
//default, isPrimaryKey
//String#byte, Number#value

/**
 * [
 *    {
 *        "name":"User",
 *        "fields":{
 *            "key1":{
 *                type:"String",
 *                default:"111",
 *                bytes: 150,
 *            },
 *            "key2":{
 *                type:"Number",
 *                default:10
 *            }
 *        },
 *        "comment":""
 *    },
 *    {
 *        "name":"Pet",
 *        "fields":{
 *            "key1":{
 *                type:"String",
 *                default:"111"
 *            },
 *            "key2":{
 *                type:"Number",
 *                default:10
 *            }
 *        },
 *        "comment":""
 *    },
 *    ['Table3','Table4','Table5']
 * ];
 * */
public class BaseDiscoverSchemaFunction extends FunctionBase {
    private static final String TAG = BaseDiscoverSchemaFunction.class.getSimpleName();

    private BaseDiscoverSchemaFunction() {
        super();
        super.functionName = JSFunctionNames.DISCOVER_SCHEMA;
    }

    public static BaseDiscoverSchemaFunction discover(LoadJavaScripter script) {
        BaseDiscoverSchemaFunction function = new BaseDiscoverSchemaFunction();
        function.javaScripter(script);
        return function;
    }

    public void invoker(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) throws Throwable {
        if (!this.javaScripter.functioned(functionName.jsName())) {
            TapLogger.info(TAG, "Not found 'discover_schema' which the implementation of a named function, cannot load and scan tables.");
            return;
        }
        SchemaSender schemaAccept = new SchemaAccept();
        schemaAccept.setConsumer(consumer);
        Object invoker;
        synchronized (JSConnector.execLock){
            invoker = this.javaScripter.invoker(
                    JSFunctionNames.DISCOVER_SCHEMA.jsName(),
                    Optional.ofNullable(connectionContext.getConnectionConfig()).orElse(new DataMap()),
                    schemaAccept);
        }
        schemaAccept.send(invoker);
    }
}

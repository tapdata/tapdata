package io.tapdata.js.connector.server.function.support;

import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public class $Temp$ extends FunctionBase implements FunctionSupport<TapFunction> {
    private $Temp$() {
        super();
        super.functionName = JSFunctionNames.BatchReadFunction;
        //        Object invoker = this.javaScripter.invoker(JSFunctionNames.DISCOVER_SCHEMA.jsName(), null);
//        if (Objects.isNull(invoker)){
//
//        }
//        List<TapTable> tables = new ArrayList<>();
//        Set<Map.Entry<String, Object>> discoverSchema = new HashSet<>();
//        try {
//            ScriptObjectMirror discoverSchemaMirror = (ScriptObjectMirror)invoker;
//            discoverSchema = discoverSchemaMirror.entrySet();
//        }catch (Exception e){
//            String tableId = String.valueOf(invoker);
//            tables.add(new TapTable(tableId,tableId));
//        }
//        if (!discoverSchema.isEmpty()) {
//            tables.addAll(discoverSchema.stream().filter(Objects::nonNull).map(entry -> {
//                String table = String.valueOf(entry.getValue());
//                return new TapTable();
//            }).collect(Collectors.toList()));
//        }
        //String
        //Object invoker1 = this.javaScripter.invoker(JSFunctionNames.BatchReadFunction.jsName(), null);
//        ScriptObjectMirror batchReadMirror = (ScriptObjectMirror)invoker1;
//        Set<Map.Entry<String, Object>> batchRead = batchReadMirror.entrySet();

        //Integer
        // Object invoker2 = this.javaScripter.invoker(JSFunctionNames.StreamReadFunction.jsName(), null);
//        ScriptObjectMirror streamReadMirror = (ScriptObjectMirror)invoker2;
//        Set<Map.Entry<String, Object>> streamRead = streamReadMirror.entrySet();

        //Object invoker3 = this.javaScripter.invoker(JSFunctionNames.CONNECTION_TEST.jsName(), null);
//        ScriptObjectMirror connectionTestMirror = (ScriptObjectMirror)invoker3;
//        Set<Map.Entry<String, Object>> connectionTest = connectionTestMirror.entrySet();

    }

    @Override
    public TapFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return null;
    }

    public static TapFunction create(LoadJavaScripter loadJavaScripter) {
        return new $Temp$().function(loadJavaScripter);
    }
}

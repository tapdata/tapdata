package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.js.connector.server.function.base.SchemaCount;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.Objects;
import java.util.Optional;

//先判断是否实现table_count,实现就返回调用结果
//未实现判断是否实现discover_schema,实现就获取tableCount
public class BaseTableCountFunction extends FunctionBase {
    private static final String TAG = BaseTableCountFunction.class.getSimpleName();
    private BaseTableCountFunction(){
        super();
        super.functionName = JSFunctionNames.TABLE_COUNT;
    }
    public static BaseTableCountFunction tableCount(LoadJavaScripter script){
        BaseTableCountFunction function = new BaseTableCountFunction();
        function.javaScripter(script);
        return function;
    }

    public int get(TapConnectionContext connectionContext){
        if (this.javaScripter.functioned(functionName.jsName())) {
            Object invoker = this.javaScripter.invoker(JSFunctionNames.TABLE_COUNT.jsName(), connectionContext);
            if (Objects.nonNull(invoker)){
                try {
                    return (Integer)invoker;
                }catch (Exception e){
                    return 0;
                }
            }
            return 0;
        }
        if (!this.javaScripter.functioned(JSFunctionNames.DISCOVER_SCHEMA.jsName())){
            TapLogger.info(TAG,"Not found 'discover_schema' which the implementation of a named function in js file [connector.js], cannot load and scan tables.");
            return 0;
        }
        SchemaCount schemaSender = new SchemaCount();
        Object invoker;
        synchronized (JSConnector.execLock) {
            invoker = this.javaScripter.invoker(
                    JSFunctionNames.DISCOVER_SCHEMA.jsName(),
                    Optional.ofNullable(connectionContext.getConnectionConfig()).orElse(new DataMap()),
                    schemaSender
            );
        }
        schemaSender.send(invoker);
        return schemaSender.get();
    }
}

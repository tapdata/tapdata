package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.js.connector.enums.JSTableKeys;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.iengine.ScriptEngineInstance;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.base.ConnectorBase.fromJsonObject;

//先判断是否实现table_count,实现就返回调用结果
//未实现判断是否实现discover_schema,实现就获取tableCount
public class BaseTableCountFunction extends FunctionBase {
    private static final String TAG = BaseTableCountFunction.class.getSimpleName();
    private BaseTableCountFunction(){
        super();
        super.functionName = JSFunctionNames.TABLE_COUNT;
    }
    public static BaseTableCountFunction tableCount(LoadJavaScripter script){
        if(Objects.isNull(script)) {
            script = ScriptEngineInstance.instance().script();
        }
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
        Object invoker = this.javaScripter.invoker(JSFunctionNames.DISCOVER_SCHEMA.jsName(), connectionContext);
        if (Objects.isNull(invoker)){
            TapLogger.info(TAG,"No table information was loaded after discoverSchema was executed.");
            return 0;
        }
        Set<Map.Entry<String, Object>> discoverSchema = new HashSet<>();
        AtomicInteger tableNum = new AtomicInteger();
        try {
            if (invoker instanceof Map){
                discoverSchema = ((Map<String,Object>)invoker).entrySet();
            }else if (invoker instanceof Collection){
                Collection<Object> tableCollection = (Collection<Object>) invoker;
                tableNum.set(tableCollection.size());
            }else {
                tableNum.getAndIncrement();
            }
        }catch (Exception e){
            tableNum.getAndIncrement();
        }
        if (!discoverSchema.isEmpty()) {
            discoverSchema.stream().filter(Objects::nonNull).forEach(entry -> {
                Object entryValue = entry.getValue();
                if (entryValue instanceof String){
                    tableNum.getAndIncrement();
                }else if (entryValue instanceof Map) {
                    Map<String,Object> tableMap = (Map<String, Object>) entryValue;
                    Object tableIdObj = tableMap.get(JSTableKeys.TABLE_NAME);
                    if (Objects.nonNull(tableIdObj)){
                        tableNum.getAndIncrement();
                    }
                }else if(entryValue instanceof Collection){
                    Collection<Object> collection = (Collection<Object>) entryValue;
                    collection.stream().filter(obj->Objects.nonNull(obj)&&"".equals(String.valueOf(obj))).forEach(table-> tableNum.getAndIncrement());
                }
            });
        }
        return tableNum.get();
    }
}

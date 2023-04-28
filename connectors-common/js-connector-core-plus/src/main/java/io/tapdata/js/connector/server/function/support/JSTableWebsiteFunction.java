package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.connection.TableWebsiteFunction;
import io.tapdata.pdk.apis.functions.connection.vo.ConnectorWebsiteData;
import io.tapdata.pdk.apis.functions.connection.vo.TableWebsiteData;
import io.tapdata.pdk.apis.functions.connection.vo.Website;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.base.ConnectorBase.toJson;

/**
 * @author GavinXiao
 * @description TableWebsiteFunction create by Gavin
 * @create 2023/4/27 16:20
 **/
public class JSTableWebsiteFunction extends FunctionBase implements FunctionSupport<TableWebsiteFunction> {
    public static final String TAG = JSTableWebsiteFunction.class.getSimpleName();

    private JSTableWebsiteFunction() {
        super();
        super.functionName = JSFunctionNames.TABLE_WEBSITE;
    }

    @Override
    public TableWebsiteFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this::website;
    }

    private Website website(TapConnectionContext context, List<String> tables) {
        if (null == context){
            TapLogger.warn(TAG,"Can not get table websites, because ConnectionContext is empty.");
            return new TableWebsiteData();
        }
        try {
            Object invoker = javaScripter.invoker(
                    JSFunctionNames.TABLE_WEBSITE.jsName(),
                    Optional.ofNullable(context.getConnectionConfig()).orElse(new DataMap()),
                    Optional.ofNullable(tables).orElse(new ArrayList<>())
            );
            if (!(invoker instanceof Map) && !(invoker instanceof String)) {
                TapLogger.warn(TAG, "Can not get table websites, can not get any data from {}.", JSFunctionNames.TABLE_WEBSITE.jsName());
                return new TableWebsiteData();
            }
            if (invoker instanceof String){
                return new TableWebsiteData().url((String) invoker);
            } else {
                return fromJson(toJson(invoker), TableWebsiteData.class);
            }
        }catch (Exception e){
            TapLogger.warn(TAG, "Can not get table websites, can not get any data from {}, msg: {}.", JSFunctionNames.TABLE_WEBSITE.jsName(), e.getMessage());
            return new TableWebsiteData();
        }
    }

    public static TableWebsiteFunction create(LoadJavaScripter loadJavaScripter) {
        return new JSTableWebsiteFunction().function(loadJavaScripter);
    }
}

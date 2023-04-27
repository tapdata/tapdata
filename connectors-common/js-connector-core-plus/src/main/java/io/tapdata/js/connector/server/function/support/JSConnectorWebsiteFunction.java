package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.connection.ConnectorWebsiteFunction;
import io.tapdata.pdk.apis.functions.connection.vo.ConnectorWebsiteData;
import io.tapdata.pdk.apis.functions.connection.vo.Website;

import java.util.Map;
import java.util.Optional;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.base.ConnectorBase.toJson;

/**
 * @author GavinXiao
 * @description JSConnectorWebsiteFunction create by Gavin
 * @create 2023/4/27 16:18
 **/
public class JSConnectorWebsiteFunction extends FunctionBase implements FunctionSupport<ConnectorWebsiteFunction> {
    public static final String TAG = JSConnectorWebsiteFunction.class.getSimpleName();
    private JSConnectorWebsiteFunction() {
        super();
        super.functionName = JSFunctionNames.CONNECTOR_WEBSITE;
    }

    @Override
    public ConnectorWebsiteFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this::website;
    }

    private Website website(TapConnectionContext context) {
        if (null == context){
            TapLogger.warn(TAG,"Can not get connector website, because ConnectionContext is empty.");
            return new ConnectorWebsiteData();
        }
        try {
            Object invoker = javaScripter.invoker(
                    JSFunctionNames.CONNECTOR_WEBSITE.jsName(),
                    Optional.ofNullable(context.getConnectionConfig()).orElse(new DataMap())
            );
            if (!(invoker instanceof Map) && !(invoker instanceof String)) {
                TapLogger.warn(TAG, "Can not get table websites, can not get any data from {}.", JSFunctionNames.TABLE_WEBSITE.jsName());
                return new ConnectorWebsiteData();
            }
            if (invoker instanceof String){
                return new ConnectorWebsiteData().url((String) invoker);
            } else {
                return fromJson(toJson(invoker), ConnectorWebsiteData.class);
            }
        }catch (Exception e){
            TapLogger.warn(TAG, "Can not get table websites, can not get any data from {}, msg: {}.", JSFunctionNames.TABLE_WEBSITE.jsName(), e.getMessage());
            return new ConnectorWebsiteData();
        }
    }

    public static ConnectorWebsiteFunction create(LoadJavaScripter loadJavaScripter) {
        return new JSConnectorWebsiteFunction().function(loadJavaScripter);
    }
}
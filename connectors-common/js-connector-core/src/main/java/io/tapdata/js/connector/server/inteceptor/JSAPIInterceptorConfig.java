package io.tapdata.js.connector.server.inteceptor;

import io.tapdata.js.connector.server.function.JSFunctionNames;

import java.util.List;
import java.util.Map;

public class JSAPIInterceptorConfig {

    public static JSAPIInterceptorConfig config() {
        return new JSAPIInterceptorConfig();
    }

    private boolean hasConfig = false;

    private List<Map<String, Object>> expireStatus;

    public static final JSFunctionNames expireFunction = JSFunctionNames.EXPIRE_STATUS;

    public static final JSFunctionNames UPDATE_TOKEN = JSFunctionNames.UPDATE_TOKEN;

    public JSAPIInterceptorConfig expireStatus(List<Map<String, Object>> expireStatus) {
        this.expireStatus = expireStatus;
        return this;
    }

    public JSAPIInterceptorConfig hasConfig(boolean hasConfig) {
        this.hasConfig = hasConfig;
        return this;
    }

    public List<Map<String, Object>> expireStatus() {
        return this.expireStatus;
    }

    public boolean hasConfig() {
        return this.hasConfig;
    }

}

package io.tapdata.pdk.apis.javascript;

import io.tapdata.pdk.apis.javascript.comom.APIIterateError;

import java.util.Map;

public interface APIIterateInterceptor {
    public boolean iterate(Map<String,Object> data, Object offset, APIIterateError error);
}

package io.tapdata.pdk.apis.javascript;

import io.tapdata.pdk.apis.javascript.entitys.APIResponse;

import java.util.Map;

public interface APIResponseInterceptor {
    APIResponse intercept(APIResponse response, String urlOrName, String method, Map<String, Object> params);
}

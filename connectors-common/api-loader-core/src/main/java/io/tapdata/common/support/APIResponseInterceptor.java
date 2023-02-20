package io.tapdata.common.support;

import io.tapdata.common.support.entitys.APIResponse;

import java.util.Map;

public interface APIResponseInterceptor {
    APIResponse intercept(APIResponse response, String urlOrName, String method, Map<String, Object> params);
    default void setInvoker(APIInvoker invoker){}
}

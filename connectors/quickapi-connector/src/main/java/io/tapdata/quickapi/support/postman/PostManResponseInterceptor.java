package io.tapdata.quickapi.support.postman;

import io.tapdata.pdk.apis.api.APIResponse;
import io.tapdata.pdk.apis.api.APIResponseInterceptor;

import java.util.Map;

public class PostManResponseInterceptor implements APIResponseInterceptor {
    @Override
    public APIResponse intercept(APIResponse response, String urlOrName, String method, Map<String, Object> params) {
        return null;
    }
}

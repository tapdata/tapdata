package io.tapdata.quickapi.support.postman;

import io.tapdata.quickapi.api.APIResponse;
import io.tapdata.quickapi.api.APIResponseInterceptor;

import java.util.Map;

public class PostManResponseInterceptor implements APIResponseInterceptor {
    @Override
    public APIResponse intercept(APIResponse response, String urlOrName, String method, Map<String, Object> params) {
        return null;
    }
}

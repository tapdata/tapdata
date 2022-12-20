package io.tapdata.common.support.postman;

import io.tapdata.common.api.APIResponse;
import io.tapdata.common.api.APIResponseInterceptor;

import java.util.Map;

public class PostManResponseInterceptor implements APIResponseInterceptor {
    @Override
    public APIResponse intercept(APIResponse response, String urlOrName, String method, Map<String, Object> params) {
        return null;
    }
}

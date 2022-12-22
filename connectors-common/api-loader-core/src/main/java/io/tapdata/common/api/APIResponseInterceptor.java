package io.tapdata.common.api;

import java.util.Map;

public interface APIResponseInterceptor {
    APIResponse intercept(APIResponse response, String urlOrName, String method, Map<String, Object> params);
}

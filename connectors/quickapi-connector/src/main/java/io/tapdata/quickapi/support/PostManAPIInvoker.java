package io.tapdata.quickapi.support;


import io.tapdata.quickapi.api.APIInvoker;
import io.tapdata.quickapi.api.APIResponse;
import io.tapdata.quickapi.api.APIResponseInterceptor;
import io.tapdata.quickapi.api.comom.APIDocument;

import java.util.Map;

public abstract class PostManAPIInvoker
        implements APIDocument<PostManAPIInvoker>, APIInvoker {
    APIResponseInterceptor interceptor;

    @Override
    public APIResponse invoke(String uriOrName, String method, Map<String, Object> params, boolean invoker) {
        APIResponse response = http(uriOrName, method, params);
        response = interceptor.intercept(response,uriOrName,method,params);
        return response;
    }

    @Override
    public void setAPIResponseInterceptor(APIResponseInterceptor interceptor) {
       this.interceptor = interceptor;
    }

    protected abstract APIResponse http(String uriOrName, String method, Map<String, Object> params);
}

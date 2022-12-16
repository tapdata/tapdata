package io.tapdata.api.invoker;

import io.tapdata.pdk.apis.api.APIInvoker;
import io.tapdata.pdk.apis.api.APIResponse;
import io.tapdata.pdk.apis.api.APIResponseInterceptor;
import io.tapdata.pdk.apis.api.comom.APIDocument;

import java.util.Map;

public abstract class PostManAPIInvoker
        implements APIDocument<PostManAPIInvoker>, APIInvoker {
    APIResponseInterceptor interceptor;

    @Override
    public APIResponse invoke(String uriOrName, String method, Map<String, Object> params,boolean invoker) {
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

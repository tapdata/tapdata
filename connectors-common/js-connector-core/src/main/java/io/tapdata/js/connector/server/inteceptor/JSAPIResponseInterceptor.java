package io.tapdata.js.connector.server.inteceptor;

import io.tapdata.common.support.APIInvoker;
import io.tapdata.common.support.APIResponseInterceptor;
import io.tapdata.common.support.entitys.APIEntity;
import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.entity.error.CoreException;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JSAPIResponseInterceptor implements APIResponseInterceptor {
    private APIInvoker invoker;
    private JSAPIInterceptorConfig config;

    public static JSAPIResponseInterceptor create(JSAPIInterceptorConfig config, APIInvoker invoker) {
        return new JSAPIResponseInterceptor().config(config).invoker(invoker);
    }

    public JSAPIResponseInterceptor invoker(APIInvoker invoker) {
        this.invoker = invoker;
        return this;
    }

    public JSAPIResponseInterceptor config(JSAPIInterceptorConfig config) {
        this.config = config;
        return this;
    }

    @Override
    public APIResponse intercept(APIResponse response, String urlOrName, String method, Map<String, Object> params) {
        if (Objects.isNull(response)) {
            throw new CoreException(String.format("Http request call failed, unable to get the request result: url or name [%s], method [%s].", urlOrName, method));
        }
        APIResponse interceptorResponse = response;
        ExpireHandel expireHandel = ExpireHandel.create(response, null, null);
        if (expireHandel.builder()) {

            List<APIEntity> apiEntities = invoker.tableApis();
            if (!apiEntities.isEmpty()) {
                APIEntity apiEntity = apiEntities.get(0);
                APIResponse tokenResponse = invoker.invoke(apiEntity.name(), params, apiEntity.method(), true);
                if (expireHandel.refreshComplete(tokenResponse, params)) {
                    //再调用
                    interceptorResponse = invoker.invoke(urlOrName, params, method, true);
                }
            }
        }
        return interceptorResponse;
    }
}
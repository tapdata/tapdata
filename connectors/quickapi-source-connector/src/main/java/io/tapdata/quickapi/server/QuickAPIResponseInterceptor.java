package io.tapdata.quickapi.server;

import io.tapdata.entity.error.CoreException;
import io.tapdata.common.support.APIInvoker;
import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.common.support.APIResponseInterceptor;
import io.tapdata.common.support.entitys.APIEntity;
import io.tapdata.quickapi.common.QuickApiConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class QuickAPIResponseInterceptor implements APIResponseInterceptor {
    private QuickApiConfig config;
    private APIInvoker invoker;
    public static QuickAPIResponseInterceptor create(QuickApiConfig config, APIInvoker invoker){
        return new QuickAPIResponseInterceptor().config(config).invoker(invoker);
    }
    public QuickAPIResponseInterceptor config(QuickApiConfig config){
        this.config = config;
        return this;
    }
    public QuickAPIResponseInterceptor invoker(APIInvoker invoker){
        this.invoker = invoker;
        return this;
    }

    @Override
    public APIResponse intercept(APIResponse response, String urlOrName, String method, Map<String, Object> params) {
        if( Objects.isNull(response) ) {
            throw new CoreException(String.format("Http request call failed, unable to get the request result: url or name [%s], method [%s].",urlOrName,method));
        }
        APIResponse interceptorResponse = response;
        ExpireHandel expireHandel = ExpireHandel.create(response, config.expireStatus(),config.tokenParams());
        if (expireHandel.builder()){
            List<APIEntity> apiEntities = invoker.tokenApis();
            if ( !apiEntities.isEmpty() ){
                APIEntity apiEntity = apiEntities.get(0);
                APIResponse tokenResponse = invoker.invoke(apiEntity.name(), params, apiEntity.method(),true);
                if (expireHandel.refreshComplete(tokenResponse,params)) {
                    //再调用
                    interceptorResponse = invoker.invoke(urlOrName, params, method,true);
                }
            }
        }
        return interceptorResponse;
    }
}

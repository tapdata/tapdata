package io.tapdata.js.connector.server.decorator;

import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;
import io.tapdata.common.support.APIResponseInterceptor;
import io.tapdata.entity.error.CoreException;
import io.tapdata.js.utils.ScriptUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class APIFactoryDecorator extends APIFactoryDecoratorStruct {
    private APIResponseInterceptor interceptor;

    public APIFactoryDecorator(APIFactory apiFactory) {
        super(apiFactory);
    }

    public APIResponseInterceptor interceptor(){
        return this.interceptor;
    }

    public APIFactoryDecorator interceptor(APIResponseInterceptor interceptor){
        this.interceptor = interceptor;
        return this;
    }

    @Override
    public APIInvoker loadAPI(String apiContent, String type, Map<String, Object> params) {
        if (Objects.isNull(type)) type = APIFactory.TYPE_POSTMAN;
        if (Objects.isNull(apiContent)){
            try {
                apiContent = ScriptUtil.fileToString("connectors-javascript/coding-test-connector/src/main/resources/postman_api_collection.json");
            } catch (IOException e) {
                throw new CoreException("File get error,may be file connectors-javascript/coding-test-connector/src/main/resources/postman_api_collection.json not exists. ");
            }
            if (Objects.isNull(apiContent)){
                throw new CoreException("File get error,may be file connectors-javascript/coding-test-connector/src/main/resources/postman_api_collection.json not exists. ");
            }
        }
        APIInvoker loadAPI = new APIInvokerDecorator(super.loadAPI(apiContent, type, params));
        loadAPI.setAPIResponseInterceptor(this.interceptor);
        return loadAPI;
    }

    @Override
    public APIInvoker loadAPI(Map<String, Object> params) {
        return this.loadAPI(null,null,params);
    }

    @Override
    public APIInvoker loadAPI() {
        return this.loadAPI(null,null,new HashMap<>());
    }
}

package io.tapdata.js.connector.server.decorator;

import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;

import java.util.Map;

public class APIFactoryDecoratorStruct implements APIFactory {
    protected APIFactory apiFactory;

    public APIFactoryDecoratorStruct(APIFactory apiFactory) {
        this.apiFactory = apiFactory;
    }

    public APIFactory apiFactory() {
        return this.apiFactory;
    }

    public APIFactoryDecoratorStruct apiFactory(APIFactory apiFactory) {
        this.apiFactory = apiFactory;
        return this;
    }

    @Override
    public APIInvoker loadAPI(String apiContent, Map<String, Object> params) {
        return apiFactory.loadAPI(apiContent, params);
    }

    @Override
    public APIInvoker loadAPI(Map<String, Object> params) {
        return apiFactory.loadAPI(params);
    }

    @Override
    public APIInvoker loadAPI() {
        return apiFactory.loadAPI();
    }
}

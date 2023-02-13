package io.tapdata.js.connector.server.decorator;

import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;
import io.tapdata.common.support.APIResponseInterceptor;
import io.tapdata.entity.error.CoreException;
import io.tapdata.common.util.ScriptUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class APIFactoryDecorator extends APIFactoryDecoratorStruct {
    private APIResponseInterceptor interceptor;
    private APIInvoker apiInvoker;

    public APIFactoryDecorator(APIFactory apiFactory) {
        super(apiFactory);
    }

    public APIResponseInterceptor interceptor() {
        return this.interceptor;
    }

    public APIFactoryDecorator interceptor(APIResponseInterceptor interceptor) {
        this.interceptor = interceptor;
        if (Objects.nonNull(this.apiInvoker)) {
            this.apiInvoker.setAPIResponseInterceptor(this.interceptor);
        }
        return this;
    }

    @Override
    public APIInvoker loadAPI(String apiContent, Map<String, Object> params) {
        if (Objects.isNull(apiContent)) {
            try {
                apiContent = ScriptUtil.loadFileFromJarPath(APIFactory.DEFAULT_POST_MAN_FILE_PATH);//ScriptUtil.fileToString("/src/main/resources/postman_api_collection.json");
            } catch (IOException e) {
                throw new CoreException("File get error,may be file connectors-javascript/../src/main/resources/postman_api_collection.json not exists. ");
            }
            if (Objects.isNull(apiContent)) {
                throw new CoreException("File get error,may be file connectors-javascript/../src/main/resources/postman_api_collection.json not exists. ");
            }
        }
        APIInvoker loadAPI = new APIInvokerDecorator(super.loadAPI(apiContent, params));
        loadAPI.setAPIResponseInterceptor(this.interceptor);
        this.apiInvoker = loadAPI;
        return loadAPI;
    }

    @Override
    public APIInvoker loadAPI(Map<String, Object> params) {
        return this.loadAPI(null, params);
    }

    @Override
    public APIInvoker loadAPI() {
        return this.loadAPI(null, new HashMap<>());
    }
}

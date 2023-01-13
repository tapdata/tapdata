package io.tapdata.js.connector.server.decorator;

import io.tapdata.common.support.APIInvoker;
import io.tapdata.common.support.APIIterateInterceptor;

public class APIInvokerDecorator extends APIInvokerDecoratorStruct {
    public APIInvokerDecorator(APIInvoker apiInvoker) {
        super(apiInvoker);
    }

    @Override
    public void iterateAllData(String urlOrName, String method, Object offset, APIIterateInterceptor interceptor) {
        super.iterateAllData(urlOrName, method, offset, interceptor);
    }
}

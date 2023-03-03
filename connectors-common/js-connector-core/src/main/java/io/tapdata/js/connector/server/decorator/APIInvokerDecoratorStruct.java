package io.tapdata.js.connector.server.decorator;

import io.tapdata.common.support.APIInvoker;
import io.tapdata.common.support.APIIterateInterceptor;
import io.tapdata.common.support.APIResponseInterceptor;
import io.tapdata.common.support.entitys.APIEntity;
import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class APIInvokerDecoratorStruct implements APIInvoker {
    private APIInvoker apiInvoker;

    public APIInvokerDecoratorStruct(APIInvoker apiInvoker) {
        this.apiInvoker = apiInvoker;
    }

    @Override
    public APIResponse invoke(String uriOrName, Map<String, Object> params, String method, boolean invoker) {
        return apiInvoker.invoke(uriOrName, params, method, invoker);
    }

    @Override
    public void setAPIResponseInterceptor(APIResponseInterceptor interceptor) {
        apiInvoker.setAPIResponseInterceptor(interceptor);
    }

    @Override
    public List<APIEntity> tableApis() {
        return apiInvoker.tableApis();
    }

    @Override
    public List<String> tables() {
        return apiInvoker.tables();
    }

    @Override
    public List<APIEntity> tokenApis() {
        return apiInvoker.tokenApis();
    }

    @Override
    public List<String> tokenApiNames() {
        return apiInvoker.tokenApiNames();
    }

    @Override
    public Map<String, Object> variable() {
        return apiInvoker.variable();
    }

    @Override
    public void setConfig(Object configMap) {
        apiInvoker.setConfig(configMap);
    }

    @Override
    public void setConnectorConfig(Object setConnectorConfig) {
        apiInvoker.setConnectorConfig(setConnectorConfig);
    }

    @Override
    public void addConnectorConfig(Object setConnectorConfig) {
        apiInvoker.addConnectorConfig(setConnectorConfig);
    }

    @Override
    public void pageStage(TapConnectorContext connectorContext, TapTable table, Object offset, int batchCount, AtomicBoolean task, BiConsumer<List<TapEvent>, Object> consumer) {
        apiInvoker.pageStage(connectorContext, table, offset, batchCount, task, consumer);
    }

    @Override
    public void iterateAllData(String urlOrName, String method, Object offset, APIIterateInterceptor interceptor) {
        apiInvoker.iterateAllData(urlOrName, method, offset, interceptor);
    }
}

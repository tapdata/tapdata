package io.tapdata.http;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author GavinXiao
 * @description HttpReceiverConnector create by Gavin
 * @create 2023/5/17 12:20
 **/
@TapConnectorClass("spec.json")
public class HttpReceiverConnector extends ConnectorBase{
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry tapCodecsRegistry) {
        connectorFunctions.supportBatchRead(this::batchRead)
                .supportRawDataCallbackFilterFunctionV2(this::callback);
    }

    private void batchRead(TapConnectorContext context, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> con) {

    }

    private List<TapEvent> callback(TapConnectorContext context, Map<String, Object> eventMap) {
        return null;
    }

    private List<TapEvent> callback(TapConnectorContext context, List<String> tableName, Map<String, Object> eventMap) {
        return null;
    }

    @Override
    public void discoverSchema(TapConnectionContext tapConnectionContext, List<String> list, int i, Consumer<List<TapTable>> consumer) throws Throwable {

    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext tapConnectionContext, Consumer<TestItem> consumer) throws Throwable {
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext tapConnectionContext) throws Throwable {
        return 1;
    }
}

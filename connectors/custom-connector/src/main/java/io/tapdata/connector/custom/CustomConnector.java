package io.tapdata.connector.custom;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec_custom.json")
public class CustomConnector extends ConnectorBase {

    private CustomConfig customConfig;

    private void initConnection(TapConnectionContext connectorContext) {
        customConfig = new CustomConfig().load(connectorContext.getConnectionConfig());
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        if (customConfig.getCustomBeforeOpr()) {

        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (customConfig.getCustomAfterOpr()) {

        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportBatchRead(this::batchRead);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        CustomSchema customSchema = new CustomSchema(customConfig);
        consumer.accept(Collections.singletonList(customSchema.loadSchema()));
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        initConnection(connectionContext);
        CustomTest customTest = new CustomTest(customConfig);
        TestItem testScript = customTest.testScript();
        consumer.accept(testScript);
        if (!ConnectionTypeEnum.TARGET.getType().equals(customConfig.get__connectionType()) && (testScript.getResult() == TestItem.RESULT_FAILED)) {
            consumer.accept(customTest.testBuildSchema());
        }
        return ConnectionOptions.create().connectionString("Custom Connection: " +
                (ConnectionTypeEnum.TARGET.getType().equals(customConfig.get__connectionType()) ? ConnectionTypeEnum.TARGET.getType() : "source[" + customConfig.getCollectionName() + "]"));
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return 1;
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {

    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {

    }

}

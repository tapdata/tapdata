package io.tapdata.connector.selectdb;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.selectdb.config.SelectDbConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Author:Skeet
 * Date: 2022/12/8 16:16
 **/

@TapConnectorClass("spec_selectdb.json")
public class SelectDbConnector extends ConnectorBase {

    private SelectDbConfig selectDbConfig;
    private SelectDbJdbcContext selectDbjdbcContext;
    private SelectDbRecordWriter recordWriter;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //        connectorFunctions.supportClearTable(this::);
        //        connectorFunctions.supportWriteRecord(this::);
        //        connectorFunctions.supportClearTable(this::);
        //        connectorFunctions.supportDropTable(this::);
        //        connectorFunctions.supportQueryByFilter(this::);
        connectorFunctions.supportBatchRead(this::batchRead)
                .supportStreamRead(this::streamRead);
    }

    private void streamRead(TapConnectorContext tapConnectorContext, List<String> strings, Object o, int i, StreamReadConsumer streamReadConsumer) {
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object o, int i, BiConsumer<List<TapEvent>, Object> listObjectBiConsumer) {
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) throws Throwable {
        selectDbConfig = (SelectDbConfig) new SelectDbConfig().load(databaseContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(selectDbConfig.getConnectionString());

        try (SelectDbTest selectDbTest = new SelectDbTest(selectDbConfig, consumer).initContext()
        ) {
            selectDbTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 0;
    }
}

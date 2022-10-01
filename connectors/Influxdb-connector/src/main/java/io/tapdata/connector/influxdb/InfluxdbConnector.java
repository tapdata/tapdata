package io.tapdata.connector.influxdb;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.influxdb.InfluxDB;

import java.util.List;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class InfluxdbConnector extends ConnectorBase {
    public static final String TAG = InfluxdbConnector.class.getSimpleName();
    
    private InfluxdbContext influxdbContext;
    private InfluxdbReader influxdbReader;
    private transient InfluxdbSchemaLoader influxdbSchemaLoader;
    
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        this.influxdbContext = new InfluxdbContext(connectionContext);
        this.influxdbReader = new InfluxdbReader(influxdbContext);
        this.influxdbSchemaLoader = new InfluxdbSchemaLoader(influxdbContext);
        TapLogger.info(TAG, "InfluxDB connector started");
    }
    
    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
    
    }
    
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
    
    }
    
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer)
            throws Throwable {
        influxdbSchemaLoader.discoverSchema(connectionContext, influxdbContext.getInfluxDBConfig(), tables, consumer, tableSize);
    }
    
    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        return null;
    }
    
    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 0;
    }
}

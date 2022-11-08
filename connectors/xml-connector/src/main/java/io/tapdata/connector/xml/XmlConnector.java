package io.tapdata.connector.xml;


import io.tapdata.common.FileConnector;
import io.tapdata.common.FileOffset;
import io.tapdata.connector.xml.config.XmlConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec_xml.json")
public class XmlConnector extends FileConnector {

    @Override
    protected void initConnection(TapConnectionContext connectorContext) throws Exception {
        fileConfig = new XmlConfig().load(connectorContext.getConnectionConfig());
        super.initConnection(connectorContext);
    }

    @Override
    protected void readOneFile(FileOffset fileOffset, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, AtomicReference<List<TapEvent>> tapEvents) throws Exception {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

    }
}

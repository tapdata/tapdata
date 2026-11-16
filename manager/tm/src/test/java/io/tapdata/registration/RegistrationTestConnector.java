package io.tapdata.registration;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.function.Consumer;

@TapConnectorClass("spec_mysql.json")
public class RegistrationTestConnector implements TapConnector {
    @Override
    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry registry) {
        functions.supportBatchRead((context, table, offset, size, consumer) -> {});
        registry.registerFromTapValue(TapStringValue.class, "varchar", null);
    }

    @Override
    public void discoverSchema(TapConnectionContext context, List<String> tables, int size,
                               Consumer<List<TapTable>> consumer) {}

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
        return ConnectionOptions.create();
    }

    @Override
    public int tableCount(TapConnectionContext context) { return 0; }

    @Override
    public void init(TapConnectionContext context) {}

    @Override
    public void stop(TapConnectionContext context) {}
}

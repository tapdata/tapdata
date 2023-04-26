package io.tapdata.connector.hive;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.codec.binary.Base64;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HiveConnector extends CommonDbConnector {

    protected HiveConfig hiveConfig;
    protected HiveJdbcContext hiveJdbcContext;

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        hiveConfig = (HiveConfig) new HiveConfig().load(connectionContext.getConnectionConfig());
        hiveJdbcContext = new HiveJdbcContext(hiveConfig);
        commonDbConfig = hiveConfig;
        jdbcContext = hiveJdbcContext;
        commonSqlMaker = new CommonSqlMaker('`');
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        EmptyKit.closeQuietly(hiveJdbcContext);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "string", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "string", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "string", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapBinaryValue.class, "string", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return new String(Base64.encodeBase64(tapValue.getValue()));
            return null;
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, "string", tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "yyyy-MM-dd"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSS"));

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        //target
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
//        connectorFunctions.supportWriteRecord(this::writeRecord);


        //source 暂不支持
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
        //query
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);

        // ddl 暂不支持
//        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
//        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
//        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
//        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> hiveJdbcContext.getConnection(), this::isAlive, c));
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext tapConnectionContext, Consumer<TestItem> consumer) {
        hiveConfig = (HiveConfig) new HiveConfig().load(tapConnectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(hiveConfig.getConnectionString());
        try (
                HiveTest hiveTest = new HiveTest(hiveConfig, consumer)
        ) {
            hiveTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    protected void singleThreadDiscoverSchema(List<DataMap> subList, Consumer<List<TapTable>> consumer) throws SQLException {
        List<TapTable> tapTableList = hiveJdbcContext.queryTablesDesc(subList.stream().map(v -> v.getString("tab_name")).collect(Collectors.toList()));
        syncSchemaSubmit(tapTableList, consumer);
    }

}

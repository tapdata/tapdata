package io.tapdata.connector.adb;

import io.tapdata.connector.postgres.PostgresConnector;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-12 15:18
 **/
@TapConnectorClass("aliyun-adb-postgres-spec.json")
public class AliyunADBPostgresConnector extends PostgresConnector {
    private PostgresConfig adbPostgresConfig;
    private PostgresJdbcContext adbPostgresJdbcContext;
    private String adbPostgresVersion;

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        super.onStart(connectorContext);
        adbPostgresConfig = (PostgresConfig) new PostgresConfig().load(connectorContext.getConnectionConfig());
        if (EmptyKit.isNull(adbPostgresJdbcContext)) {
            adbPostgresJdbcContext = new PostgresJdbcContext(adbPostgresConfig);
        }
        adbPostgresVersion = adbPostgresJdbcContext.queryVersion();
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        super.registerCapabilities(connectorFunctions, codecRegistry);
        connectorFunctions.supportWriteRecord(this::writeRecord);
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        String insertDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        new AliyunADBPostgresRecordWriter(adbPostgresJdbcContext, tapTable)
                .setVersion(adbPostgresVersion)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .write(tapRecordEvents, writeListResultConsumer);
    }
}

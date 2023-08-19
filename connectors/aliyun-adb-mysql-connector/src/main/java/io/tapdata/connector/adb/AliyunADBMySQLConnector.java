package io.tapdata.connector.adb;

import io.tapdata.connector.adb.write.AliyunADBBatchWriter;
import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.writer.MysqlWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-12 15:18
 **/
@TapConnectorClass("aliyun-adb-mysql-spec.json")
public class AliyunADBMySQLConnector extends MysqlConnector {
    private static final String TAG = AliyunADBMySQLConnector.class.getSimpleName();
    private MysqlWriter mysqlWriter;
    private MysqlJdbcContextV2 aliyunADBJdbcContext;

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        tapConnectionContext.getConnectionConfig().put("protocolType", "mysql");
        super.onStart(tapConnectionContext);
        this.aliyunADBJdbcContext = new MysqlJdbcContextV2(new MysqlConfig().load(tapConnectionContext.getConnectionConfig()));
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.mysqlWriter = new AliyunADBBatchWriter(aliyunADBJdbcContext);
        }
    }

    @Override
    public void onStop(TapConnectionContext tapConnectionContext) {
        super.onStop(tapConnectionContext);
        try {
            Optional.ofNullable(this.mysqlWriter).ifPresent(MysqlWriter::onDestroy);
        } catch (Exception ignored) {
        }
        if (null != aliyunADBJdbcContext) {
            try {
                this.aliyunADBJdbcContext.close();
                this.aliyunADBJdbcContext = null;
            } catch (Exception e) {
                TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
            }
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        super.registerCapabilities(connectorFunctions, codecRegistry);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportStreamRead(null);
        connectorFunctions.supportTimestampToStreamOffset(null);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        mysqlConfig = new MysqlConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(mysqlConfig.getConnectionString());
        try (
                AliyunADBMySQLTest aliyunADBMySQLTest = new AliyunADBMySQLTest(mysqlConfig, consumer)
        ) {
            aliyunADBMySQLTest.testOneByOne();
        }
        return connectionOptions;
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = this.mysqlWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
        consumer.accept(writeListResult);
    }
}

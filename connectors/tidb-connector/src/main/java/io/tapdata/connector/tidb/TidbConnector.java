package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.mysql.bean.MysqlColumn;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.connector.tidb.ddl.TidbDDLSqlGenerator;
import io.tapdata.connector.tidb.dml.TidbReader;
import io.tapdata.connector.tidb.dml.TidbRecordWriter;
import io.tapdata.connector.tidb.util.HttpUtil;
import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Pattern;


@TapConnectorClass("spec_tidb.json")
public class TidbConnector extends CommonDbConnector {
    private static final String TAG = TidbConnector.class.getSimpleName();
    private TidbConfig tidbConfig;
    private TidbJdbcContext tidbJdbcContext;
    private TicdcKafkaService ticdcKafkaService;
    private TimeZone timezone;
    private TidbReader tidbReader;
    private HttpUtil httpUtil;
    private String changeFeedId;

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        this.tidbConfig = new TidbConfig().load(tapConnectionContext.getConnectionConfig());
        tidbJdbcContext = new TidbJdbcContext(tidbConfig);
        commonDbConfig = tidbConfig;
        jdbcContext = tidbJdbcContext;
        if (EmptyKit.isBlank(tidbConfig.getTimezone())) {
            timezone = tidbJdbcContext.queryTimeZone();
        }
        commonSqlMaker = new CommonSqlMaker('`');
        tidbReader = new TidbReader(tidbJdbcContext);
        ddlSqlGenerator = new TidbDDLSqlGenerator();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportReleaseExternalFunction(this::onDestroy);
        // target functions
        connectorFunctions.supportCreateTableV2(this::createTableV3);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);

        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> tidbJdbcContext.getConnection(), c));

        // source functions
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadV3);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);

        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (tapDateTimeValue.getValue() != null && tapDateTimeValue.getValue().getTimeZone() == null) {
                tapDateTimeValue.getValue().setTimeZone(timezone);
            }
            return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
            if (tapDateValue.getValue() != null && tapDateValue.getValue().getTimeZone() == null) {
                tapDateValue.getValue().setTimeZone(timezone);
            }
            return formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd");
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTimeStr());
        codecRegistry.registerFromTapValue(TapYearValue.class, tapYearValue -> {
            if (tapYearValue.getValue() != null && tapYearValue.getValue().getTimeZone() == null) {
                tapYearValue.getValue().setTimeZone(timezone);
            }
            return formatTapDateTime(tapYearValue.getValue(), "yyyy");
        });
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);

        codecRegistry.registerFromTapValue(TapMapValue.class, "longtext", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "longtext", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });

    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        KafkaConfig kafkaConfig = (KafkaConfig) new KafkaConfig().load(nodeContext.getConnectionConfig());
        httpUtil = new HttpUtil();
        changeFeedId = (String) nodeContext.getStateMap().get("changeFeedId");
        ChangeFeed changefeed = new ChangeFeed();
        if (EmptyKit.isNull(changeFeedId)) {
            changeFeedId = UUID.randomUUID().toString().replaceAll("-", "");
            if (Pattern.matches("^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$", changeFeedId)) {
                nodeContext.getStateMap().put("changeFeedId", changeFeedId);
                changefeed.setSinkUri("kafka://" + kafkaConfig.getNameSrvAddr() + "/" + tidbConfig.getMqTopic() + "?" + "kafka-version=2.4.0&partition-num=1&max-message-bytes=67108864&replication-factor=1&protocol=canal-json&auto-create-topic=true");
                changefeed.setChangeFeedId(changeFeedId);
                changefeed.setForceReplicate(true);
                changefeed.setSyncDdl(true);
                if (httpUtil.createChangefeed(changefeed, tidbConfig.getTicdcUrl())) {
                    ticdcKafkaService = new TicdcKafkaService(kafkaConfig, tidbConfig);
                    ticdcKafkaService.streamConsume(tableList, recordSize, consumer);
                }
            }
        } else {
            if (httpUtil.resumeChangefeed(changeFeedId, tidbConfig.getTicdcUrl())) {
                ticdcKafkaService = new TicdcKafkaService(kafkaConfig, tidbConfig);
                ticdcKafkaService.streamConsume(tableList, recordSize, consumer);
            }
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        if (EmptyKit.isNull(offsetStartTime)) {
            return System.currentTimeMillis();
        }
        return offsetStartTime;
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Exception {
        EmptyKit.closeQuietly(tidbJdbcContext);
        EmptyKit.closeQuietly(ticdcKafkaService);
        if (EmptyKit.isNotNull(httpUtil)) {
            if (!httpUtil.isChangeFeedClosed()) {
                httpUtil.pauseChangefeed(changeFeedId, tidbConfig.getTicdcUrl());
            }
            EmptyKit.closeQuietly(httpUtil);
        }
    }

    private void onDestroy(TapConnectorContext connectorContext) throws Throwable {
        if (EmptyKit.isNotNull(changeFeedId)) {
            httpUtil.deleteChangefeed(changeFeedId, tidbConfig.getTicdcUrl());
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        new TidbRecordWriter(tidbJdbcContext, tapTable).write(tapRecordEvents, consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) {
        tidbConfig = new TidbConfig().load(databaseContext.getConnectionConfig());
        KafkaConfig kafkaConfig = (KafkaConfig) new KafkaConfig().load(databaseContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(tidbConfig.getConnectionString());
        try (
                TidbConnectionTest connectionTest = new TidbConnectionTest(tidbConfig, consumer, connectionOptions)
        ) {
            connectionTest.setKafkaConfig(kafkaConfig);
            connectionTest.testOneByOne();
        }
        return connectionOptions;
    }

    private void queryByAdvanceFilter(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) {
        FilterResults filterResults = new FilterResults();
        filterResults.setFilter(tapAdvanceFilter);
        try {
            tidbReader.readWithFilter(tapConnectorContext, tapTable, tapAdvanceFilter, n -> !isAlive(), data -> {
                filterResults.add(data);
                if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                    consumer.accept(filterResults);
                    filterResults.getResults().clear();
                }
            });
            if (CollectionUtils.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
                filterResults.getResults().clear();
            }
        } catch (Throwable e) {
            filterResults.setError(e);
            consumer.accept(filterResults);
        }
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new MysqlColumn(dataMap).getTapField();
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) throws Throwable {
        DataMap dataMap = tidbJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }
}


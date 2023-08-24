package io.tapdata.connector.redis;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.redis.constant.ValueTypeEnum;
import io.tapdata.connector.redis.exception.RedisExceptionCollector;
import io.tapdata.connector.redis.writer.AbstractRedisRecordWriter;
import io.tapdata.connector.redis.writer.HashRedisRecordWriter;
import io.tapdata.connector.redis.writer.ListRedisRecordWriter;
import io.tapdata.connector.redis.writer.StringRedisRecordWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_redis.json")
public class RedisConnector extends ConnectorBase {

    private final static String INIT_TABLE_NAME = "tapdata";
    private final static String INIT_FIELD_NAME = "redis_cmd";
    private RedisConfig redisConfig;
    private RedisContext redisContext;
    private RedisExceptionCollector redisExceptionCollector;

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        EmptyKit.closeQuietly(redisContext);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        TapTable dbTable = table(INIT_TABLE_NAME);
        dbTable.add(field(INIT_FIELD_NAME, "string"));
        consumer.accept(Collections.singletonList(dbTable));
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return 1;
    }

    public void initConnection(TapConnectionContext connectionContext) throws Throwable {
        this.redisConfig = new RedisConfig();
        redisConfig.load(connectionContext.getConnectionConfig());
        redisConfig.load(connectionContext.getNodeConfig());
        this.redisContext = new RedisContext(redisConfig);
        this.redisExceptionCollector = new RedisExceptionCollector();
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportWriteRecord(this::writeRecord);
//        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportBatchRead(this::batchRead);

        // TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        redisConfig = new RedisConfig().load(connectionContext.getConnectionConfig());
        RedisTest redisTest = new RedisTest(redisConfig);
        TestItem testHostPort = redisTest.testHostPort();
        consumer.accept(testHostPort);
        if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
            redisTest.close();
            return null;
        }
        TestItem testConnect = redisTest.testConnect();
        consumer.accept(testConnect);
        redisTest.close();
        return null;
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        AbstractRedisRecordWriter recordWriter;
        switch (ValueTypeEnum.fromString(redisConfig.getValueType())) {
            case LIST:
                recordWriter = new ListRedisRecordWriter(redisContext, tapTable);
                break;
            case HASH:
                recordWriter = new HashRedisRecordWriter(redisContext, tapTable);
                break;
            default:
                recordWriter = new StringRedisRecordWriter(redisContext, tapTable);
        }
        try {
            recordWriter.write(tapRecordEvents, writeListResultConsumer);
        } catch (Throwable e) {
            redisExceptionCollector.collectRedisServerUnavailable(e);
            throw e;
        }
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        if (redisConfig.getOneKey()) {
            cleanOneKey(tapClearTableEvent.getTableId());
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        if (redisConfig.getOneKey()) {
            cleanOneKey(tapDropTableEvent.getTableId());
        }
    }

    private void cleanOneKey(String keyName) {
        if (EmptyKit.isBlank(keyName)) {
            return;
        }
        try (CommonJedis jedis = redisContext.getJedis()) {
            jedis.del(keyName);
            if (ValueTypeEnum.fromString(redisConfig.getValueType()) == ValueTypeEnum.HASH) {
                jedis.hdel(redisConfig.getSchemaKey(), keyName);
            }
        }
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent createTableEvent) {
        switch (ValueTypeEnum.fromString(redisConfig.getValueType())) {
            case STRING:
            case SET:
            case ZSET:
                return;
        }
        if (!redisConfig.getListHead()) {
            return;
        }
        try (CommonJedis jedis = redisContext.getJedis()) {
            List<String> fieldList = createTableEvent.getTable().getNameFieldMap().entrySet().stream().sorted(Comparator.comparing(v ->
                    EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList());
            if (redisConfig.getOneKey()) {
                String keyName = createTableEvent.getTableId();
                if (ValueTypeEnum.fromString(redisConfig.getValueType()) == ValueTypeEnum.LIST) {
                    jedis.del(keyName);
                    jedis.rpush(keyName, String.join(EmptyKit.isEmpty(redisConfig.getValueJoinString()) ? "," : redisConfig.getValueJoinString(), fieldList));
                } else {
                    jedis.hset(redisConfig.getSchemaKey(), keyName, fieldList.stream().map(v -> csvFormat(v, ",")).collect(Collectors.joining(",")));
                }
            }
        }
    }

    private String csvFormat(String str, String delimiter) {
        if (str.contains(delimiter)
                || str.contains("\t")
                || str.contains("\r")
                || str.contains("\n")
                || str.contains(" ")
                || str.contains("\"")) {
            return "\"" + str.replaceAll("\"", "\"\"") + "\"";
        }
        return str;
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {

    }
}

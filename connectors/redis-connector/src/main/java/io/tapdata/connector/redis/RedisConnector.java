package io.tapdata.connector.redis;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.function.Consumer;

/**
 * @author lemon
 */
@TapConnectorClass("spec_redis.json")
public class RedisConnector extends ConnectorBase {

    private RedisConfig redisConfig;

    private RedisContext redisContext;

    private  final  static String INIT_TABLE_NAME="tapdata";




    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {

       this.redisConfig = new RedisConfig().load(connectionContext.getConnectionConfig());
       this.redisContext = new RedisContext(redisConfig);

    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        redisContext.close();
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateTable(this::createTable);
        codecRegistry.registerFromTapValue(TapTimeValue.class, "datetime", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null){
                return toJson(tapValue.getValue());
            }
            return "null";
        });

        // TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());

    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        List<TapTable> tapTableList = list();
        TapTable tapTable  = new TapTable();
        tapTable.setName(INIT_TABLE_NAME);
        tapTableList.add(tapTable);
        consumer.accept(tapTableList);
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

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 1;
    }


    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        new RedisRecordWriter(redisContext, tapTable, connectorContext).write(tapRecordEvents, tapTable, writeListResultConsumer);
    }


    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {

    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        DataMap nodeConfig = tapConnectorContext.getNodeConfig();
        String keyName = tapDropTableEvent.getTableId();
        if(nodeConfig !=null) {
           keyName = (String) nodeConfig.get("cachePrefix");
        }
        Jedis jedis = redisContext.getJedis();
        try {
            jedis.del(keyName);
        }finally {
            jedis.close();
        }
    }


    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent createTableEvent) throws Throwable {

        DataMap nodeConfig = tapConnectorContext.getNodeConfig();
        // 存储为json模式时，不需要保存schema
        if(nodeConfig != null){
            String  valueType = (String) nodeConfig.get("valueType");
            if(!RedisRecordWriter.VALUE_TYPE_LIST.equals(valueType)){
                return;
            }
        }

        Jedis jedis = redisContext.getJedis();
        try {
            // 获取源表的字段
            List<TapField> fieldList = new ArrayList<>();
            LinkedHashMap<String, TapField> hashMap = createTableEvent.getTable().getNameFieldMap();

            for (Map.Entry<String, TapField> entry : hashMap.entrySet()) {
                fieldList.add(entry.getValue());
            }

            // 拼装数据库中的schema结构，根据tapFiled pos排序
            String schema = "";
            Collections.sort(fieldList, Comparator.comparing(TapField::getPos));
            for (TapField tapField : fieldList) {
                schema += "," + tapField.getName();
            }
            schema = schema.substring(1);

            // redis key的表名。如果前缀表名不存在，目标定义的表名
            String keyName = createTableEvent.getTableId();
            if (nodeConfig != null && nodeConfig.get("cachePrefix") != null) {
                keyName = (String) nodeConfig.get("cachePrefix");
            }

            jedis.del(keyName);
            // 给redis第一行写入schema
            jedis.rpush(keyName, schema);
        }finally {
            jedis.close();
        }

    }
}

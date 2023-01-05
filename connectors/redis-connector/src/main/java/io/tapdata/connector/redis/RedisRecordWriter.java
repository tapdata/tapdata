package io.tapdata.connector.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.*;
import java.util.function.Consumer;

/**
 * @author lemon
 */
public class RedisRecordWriter {


    private final Jedis jedis;

    private final RedisContext redisContext;

    public final static String VALUE_TYPE_LIST = "list";

    private final TapConnectorContext connectorContext;

    private Map<String, Integer> filedMap;

    private TapTable tapTable;


    public RedisRecordWriter(RedisContext redisContext, TapTable tapTable, TapConnectorContext connectorContext) {
        this.redisContext = redisContext;
        this.jedis = redisContext.getJedis();
        this.connectorContext = connectorContext;
        this.tapTable = tapTable;
        if (MapUtils.isEmpty(filedMap)) {
            this.filedMap = sortFiled(tapTable);
        }

    }

    /**
     * json 格式写入数据
     *
     * @param tapRecordEvents
     * @param tapTable
     * @param writeListResultConsumer
     */
    public void write(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {

        DataMap nodeConfig = connectorContext.getNodeConfig();
        String valueType;
        if (nodeConfig == null) {
            valueType = VALUE_TYPE_LIST;
        } else {
            valueType = (String) nodeConfig.get("valueType");
        }

        if (VALUE_TYPE_LIST.equals(valueType)) {
            handleListData(tapRecordEvents, writeListResultConsumer);
        } else {

            handleJsonData(tapRecordEvents, writeListResultConsumer);
        }
    }


    /**
     * 处理json格式的数据
     */
    private void handleJsonData(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {
        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        long insert = 0L;
        long update = 0L;
        long delete = 0L;
        Pipeline pipelined = jedis.pipelined();
        try {
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                if (recordEvent instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    Map<String, Object> value = tapInsertRecordEvent.getAfter();
                    String tableName = tapInsertRecordEvent.getTableId();
                    writeData(value, tapInsertRecordEvent, pipelined, tableName);
                    insert++;
                } else if (recordEvent instanceof TapUpdateRecordEvent) {
                    TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                    Map<String, Object> value = tapUpdateRecordEvent.getAfter();
                    String tableName = tapUpdateRecordEvent.getTableId();
                    writeData(value, tapUpdateRecordEvent, pipelined, tableName);
                    update++;

                } else {
                    TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                    Map<String, Object> value = tapDeleteRecordEvent.getBefore();
                    String tableName = tapDeleteRecordEvent.getTableId();
                    writeData(value, tapDeleteRecordEvent, pipelined, tableName);
                    delete++;
                }

            }

            pipelined.sync();
            writeListResultConsumer.accept(listResult
                    .insertedCount(insert)
                    .modifiedCount(update)
                    .removedCount(delete));

        } catch (Exception e) {
            TapLogger.error("Redis write failed,message: {}", e.getMessage(), e);
            throw e;
        } finally {
            jedis.close();
        }
    }

    /**
     * 组装pipeline数据
     */
    private void writeData(Map<String, Object> value, TapRecordEvent recordEvent, Pipeline pipelined, String tableName) throws Exception {
        if (MapUtils.isEmpty(value)) {
            TapLogger.warn("Message data is empty {} will skip it.", JSON.toJSONString(recordEvent));
            return;
        }
        String key = redisContext.getRedisKeySetter().getRedisKey(value, connectorContext, tableName);
        // json格式平铺
        String strValue = getJsonValue(value);
        if (recordEvent instanceof TapDeleteRecordEvent) {
            pipelined.del(key);
        } else {
            pipelined.set(key, strValue);
        }

    }

    /**
     * handle data for json
     * @return String
     */
    private String getJsonValue(Map<String, Object> map) throws Exception {

        JSONObject jsonObject  =new JSONObject();
        for (String key : map.keySet()) {
            Object value;
            if (null == map.get(key)) {
                value = "null";
            } else {
                value = map.get(key);
            }
            jsonObject.put(key,value);

        }
        return jsonObject.toJSONString();
    }



    /**
     * handle data for list
     *
     * @return String
     */
    private String getStrValue(Map<String, Object> map) throws Exception {
        if (MapUtils.isEmpty(filedMap)) {
            sortFiled(tapTable);
        }

        String[] filedValue = new String[filedMap.size()];
        for (String key : map.keySet()) {
            Object value;
            if (null == map.get(key)) {
                value = "null";
            } else {
                value = map.get(key);
            }

            if(filedMap.get(key) ==null){
                throw  new Exception("schema has change ,please reset task ");
            }
            int index = filedMap.get(key);
            String valueStr = String.valueOf(value);
            // 逗号作为分隔符, 存在则按照整个字段加双引号
//            if (valueStr.indexOf(",") > -1) {
//                valueStr ="\""+ valueStr+"\"";
//            }

            filedValue[index] = valueStr;
        }
        return StringUtils.join(filedValue, ",");
    }


    /**
     * 处理list格式的数据
     */
    private void handleListData(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {

        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        long insert = 0L;
        long update = 0L;
        long delete = 0L;
        Pipeline pipelined = jedis.pipelined();
        try {
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                if (recordEvent instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    Map<String, Object> value = tapInsertRecordEvent.getAfter();
                    String tableName = tapInsertRecordEvent.getTableId();
                    String keyName = redisContext.getRedisKeySetter().getRedisKey(value, connectorContext, tableName);
                    String strValue = getStrValue(value);
                    pipelined.rpush(keyName, strValue);
                    insert++;
                }
                if (recordEvent instanceof TapUpdateRecordEvent) {
                    TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                    Map<String, Object> afterValue = tapUpdateRecordEvent.getAfter();
                    String tableName = tapUpdateRecordEvent.getTableId();
                    String keyName = redisContext.getRedisKeySetter().getRedisKey(afterValue, connectorContext, tableName);
                    String newValue = getStrValue(afterValue);
                    if(null == tapUpdateRecordEvent.getBefore()){
                        throw new Exception("Redis update failed  reason before data is null");
                    }
                    String oldValue = getStrValue(tapUpdateRecordEvent.getBefore());

                    pipelined.eval("local pos = redis.call('lpos', KEYS[1], ARGV[1]); if (not pos) then return end; redis.call('lset', KEYS[1], pos, ARGV[2]);", 1, keyName, oldValue, newValue);
                    update++;
                }

                if (recordEvent instanceof TapDeleteRecordEvent) {
                    TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                    Map<String, Object> value = tapDeleteRecordEvent.getBefore();
                    String tableName = tapDeleteRecordEvent.getTableId();
                    String keyName = redisContext.getRedisKeySetter().getRedisKey(value, connectorContext, tableName);
                    String oldValue = getStrValue(value);
                    pipelined.lrem(keyName,1,oldValue);
                    delete++;
                }
            }

            pipelined.sync();

            writeListResultConsumer.accept(listResult
                    .insertedCount(insert)
                    .modifiedCount(update)
                    .removedCount(delete));

        } catch (Exception e) {
            TapLogger.error("Redis write failed,message: {}", e.getMessage(), e);
            throw e;
        } finally {
            jedis.close();
        }
    }


    /**
     * DB的字段进行排序安札pos升序排序
     *
     * @param tapTable
     * @return
     */
    public static Map<String, Integer> sortFiled(TapTable tapTable) {
        Map<String, Integer> filed = new HashMap<>(32);
        List<TapField> fieldList = new ArrayList<>();
        LinkedHashMap<String, TapField> hashMap = tapTable.getNameFieldMap();
        for (Map.Entry<String, TapField> entry : hashMap.entrySet()) {
            fieldList.add(entry.getValue());
        }
        Collections.sort(fieldList, Comparator.comparing(TapField::getPos));
        for (int index = 0; index < fieldList.size(); index++) {
            filed.put(fieldList.get(index).getName(), index);
        }
        return filed;
    }
}

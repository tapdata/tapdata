package io.tapdata.connector.redis;

import io.tapdata.connector.constant.RedisKey;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author lemon
 */
public class RedisRecordWriter {


    private Jedis jedis;
    private final TapTable tapTable;

    private WriteListResult<TapRecordEvent> listResult;
    private final Map<String, TapRecordEvent> eventMap = new HashMap<>();

    public RedisRecordWriter(RedisContext redisContext, TapTable tapTable) throws Throwable {
        this.jedis = redisContext.getJedis();
        this.tapTable = tapTable;
    }


    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        //result of these events
        listResult = new WriteListResult<>();
        long insert = 0L;
        long update = 0L;
        long delete = 0L;
        Pipeline pipelined = jedis.pipelined();
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            Map<String, String> newMap = new HashMap<>(16);
            if (recordEvent instanceof TapInsertRecordEvent) {
                insert++;
            }else if(recordEvent instanceof  TapUpdateRecordEvent){
                update++;

            }else{
                delete++;

            }




        }
        writeListResultConsumer.accept(listResult
                .insertedCount(insert)
                .modifiedCount(update)
                .removedCount(delete));
    }




    private String object2String(Object object) {
        if (object == null) {
            return "null";
        }
        if (object instanceof byte[]) {
            return new String((byte[]) object);
        }
        return object.toString();
    }



}

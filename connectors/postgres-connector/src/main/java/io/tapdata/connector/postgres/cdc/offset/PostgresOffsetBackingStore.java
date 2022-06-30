package io.tapdata.connector.postgres.cdc.offset;

import io.tapdata.kit.EmptyKit;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JsonParser;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresOffsetBackingStore extends MemoryOffsetBackingStore {

    private PostgresOffset postgresOffset;
    private WorkerConfig config;

    public PostgresOffsetBackingStore() {
    }

    public void configure(WorkerConfig config) {
        super.configure(config);
        this.config = config;
        String slotName = (String) config.originals().get("slot.name");
        this.postgresOffset = PostgresOffsetStorage.postgresOffsetMap.get(slotName);
    }

    public synchronized void start() {
        super.start();
        this.load();
    }

    public synchronized void stop() {
        super.stop();
    }

    private void load() {
        if (EmptyKit.isNull(postgresOffset) || EmptyKit.isNull(postgresOffset.getSourceOffset())) {
            this.data = new HashMap<>();
        } else {
//            this.data.put(ByteBuffer.wrap(getOffsetKey().getBytes()), ByteBuffer.wrap(postgresOffset.getSourceOffset().getBytes()));
            this.data.put(ByteBuffer.wrap(getOffsetKey().getBytes()),
                    ByteBuffer.wrap(postgresOffset.getSourceOffset().getBytes()));
        }
//        System.out.println(getOffsetKey());
//        System.out.println(postgresOffset.getSourceOffset());
    }

    private String getOffsetKey() {
        Map<String, Object> map = new HashMap<>();
        map.put("schema", null);
        List<Object> list = TapSimplify.list();
        list.add(config.originals().get("name"));
        Map<String, Object> map1 = new HashMap<>();
        map1.put("server", config.originals().get("database.dbname"));
        list.add(map1);
        map.put("payload", list);
        return TapSimplify.toJson(map, JsonParser.ToJsonFeature.WriteMapNullValue);
    }

    protected void save() {
//        this.data.forEach((key, value) -> {
//            if (EmptyKit.isNotNull(key)) {
//                postgresOffset.setStreamOffsetKey(new String(key.array()));
//                postgresOffset.setStreamOffsetValue(new String(value.array()));
//                JSONObject jsonObject = JSONObject.parseObject(postgresOffset.getStreamOffsetValue());
//                postgresOffset.setStreamOffsetTime(jsonObject.getLong("ts_usec"));
//            }
//        });
//        PostgresOffsetStorage.postgresOffsetMap.put(slotName, postgresOffset);
//        if (EmptyKit.isNull(PostgresOffsetStorage.manyOffsetMap.get(slotName))) {
//            PostgresOffsetStorage.manyOffsetMap.put(slotName, Collections.singletonList(postgresOffset));
//        } else {
//            PostgresOffsetStorage.manyOffsetMap.get(slotName).add(postgresOffset);
//        }
//        System.out.println(postgresOffset.getStreamOffsetKey());
//        System.out.println(postgresOffset.getStreamOffsetValue());
    }

}

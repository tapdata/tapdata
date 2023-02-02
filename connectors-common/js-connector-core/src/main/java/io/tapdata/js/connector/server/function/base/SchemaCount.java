package io.tapdata.js.connector.server.function.base;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.js.connector.enums.JSTableKeys;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class SchemaCount implements SchemaSender {
    private static final String TAG = SchemaCount.class.getSimpleName();

    private int tableCount;

    @Override
    public void send(Object schemaObj) {
        if (Objects.isNull(schemaObj)) {
            return;
        }
        Set<Map.Entry<String, Object>> discoverSchema = new HashSet<>();
        AtomicInteger tableNum = new AtomicInteger();
        try {
            if (schemaObj instanceof Map) {
                discoverSchema = ((Map<String, Object>) schemaObj).entrySet();
            } else if (schemaObj instanceof Collection) {
                Collection<Object> tableCollection = (Collection<Object>) schemaObj;
                tableNum.set(tableCollection.size());
            } else {
                tableNum.getAndIncrement();
            }
        } catch (Exception e) {
            tableNum.getAndIncrement();
        }
        if (!discoverSchema.isEmpty()) {
            discoverSchema.stream().filter(Objects::nonNull).forEach(entry -> {
                Object entryValue = entry.getValue();
                if (entryValue instanceof String) {
                    tableNum.getAndIncrement();
                } else if (entryValue instanceof Map) {
                    Map<String, Object> tableMap = (Map<String, Object>) entryValue;
                    Object tableIdObj = tableMap.get(JSTableKeys.TABLE_NAME);
                    if (Objects.nonNull(tableIdObj)) {
                        tableNum.getAndIncrement();
                    }
                } else if (entryValue instanceof Collection) {
                    Collection<Object> collection = (Collection<Object>) entryValue;
                    collection.stream().filter(obj -> Objects.nonNull(obj) && "".equals(String.valueOf(obj))).forEach(table -> tableNum.getAndIncrement());
                }
            });
        }
        this.tableCount += tableNum.get();
    }

    @Override
    public void setConsumer(Consumer<List<TapTable>> consumer) {

    }

    public int get() {
        return this.tableCount;
    }
}

package io.tapdata.js.connector.server.sender;

import io.tapdata.common.support.APISender;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.base.EventType;
import io.tapdata.js.connector.base.ScriptCore;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.utils.Collector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.toJson;

public class StreamReadSender implements APISender {
    private static final String TAG = StreamReadSender.class.getSimpleName();

    private ScriptCore core;

    public StreamReadSender core(ScriptCore core) {
        this.core = core;
        return this;
    }

    @Override
    public void send(Object offset) {
        core.updateOffset(offset);
    }

    @Override
    public void send(Object data, String tableName, Object offset) {
        this.send(data, tableName, EventType.insert, offset);
    }

    @Override
    public void send(Object data, String tableName, String eventType, Object offset) {
        if (Objects.isNull(core)) {
            TapLogger.warn(TAG, "ScriptCore can not be null or not be empty.");
            return;
        }
        if (Objects.isNull(data)) {
            if (offset != null)
                core.updateOffset(offset);
            return;
        }
        data = LoadJavaScripter.covertData(data);
        core.push(this.covertList(Collector.convertObj(data), tableName), eventType, Optional.ofNullable(offset).orElse(new HashMap<>()));
    }

    Map<Integer, Object> cacheAgoData = new ConcurrentHashMap<>();

    @Override
    public void send(Object data, String tableName, Object offset, boolean cacheAgoRecord) {
        if (!cacheAgoRecord) this.send(data, tableName, offset);
        Collection<Object> listData;
        if (data instanceof Collection) {
            listData = (Collection<Object>) data;
        } else {
            listData = new ArrayList<>();
            listData.add(data);
        }
        List<Object> newData = listData.stream().filter(dataItem -> {
                    Integer hashCode = this.hashCode(dataItem);
                    return -1 != hashCode && Objects.isNull(cacheAgoData.get(hashCode));
                }
        ).collect(Collectors.toList());
        cacheAgoData = new ConcurrentHashMap<>();
        newData.stream().filter(Objects::nonNull).forEach(item -> cacheAgoData.put(this.hashCode(item), item));
        this.send(data, tableName, offset);
    }

    private Integer hashCode(Object obj) {
        if (Objects.isNull(obj)) return -1;
        return toJson(obj).hashCode();
    }

    @Override
    public List<Object> covertList(Object obj, String tableName) {
        List<Object> list = new ArrayList<>();
        if (obj instanceof Collection) {
            Collection<Object> collection = (Collection<Object>) obj;
            for (Object data : collection) {
                list.add(EventType.defaultEventData(data, tableName));
            }
        } else if (obj instanceof Map) {
            list.add(EventType.defaultEventData(obj, tableName));
        } else {
            throw new CoreException("Article record:  The event format is incorrect. Please use the following rules to organize the returned results :\n" +
                    "{\n" +
                    "\"eventType\": String('i/u/d'),\n" +
                    " \"tableName\": String('example_table_name'), " +
                    "\n\"beforeData\": {}," +
                    "\n\"afterData\": {}," +
                    "\n\"referenceTime\": Number(time_stamp)" +
                    "}\n");
        }
        return list;
    }
}

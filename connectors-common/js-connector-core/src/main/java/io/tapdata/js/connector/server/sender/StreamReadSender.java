package io.tapdata.js.connector.server.sender;

import io.tapdata.common.support.APISender;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.base.EventType;
import io.tapdata.js.connector.base.ScriptCore;
import io.tapdata.js.connector.iengine.LoadJavaScripter;

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
    public void send(Object data, String tableName) {
        this.send(data, tableName, EventType.insert);
    }

    @Override
    public void send(Object data, String tableName, String eventType) {
        if (Objects.isNull(core)) {
            TapLogger.warn(TAG, "ScriptCore can not be null or not be empty.");
            return;
        }
        data = LoadJavaScripter.covertData(data);
        core.push(this.covertList(data, tableName), eventType, new HashMap<>());
    }

    Map<Integer, Object> cacheAgoData = new ConcurrentHashMap<>();

    @Override
    public void send(Object data, String tableName, boolean cacheAgoRecord) {
        if (!cacheAgoRecord) this.send(data, tableName);
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
        this.send(data, tableName);
    }

    private Integer hashCode(Object obj) {
        if (Objects.isNull(obj)) return -1;
        return toJson(obj).hashCode();
    }

    @Override
    public List<Object> covertList(Object obj, String tableName) {
        List<Object> list = new ArrayList<>();
        EventType eventType = new EventType();
        int index = 1;
        if (obj instanceof Collection) {
            Collection<Object> collection = (Collection<Object>) obj;
            for (Object data : collection) {
                list.add(eventType.setEvent(EventType.defaultEventData(data, tableName),index,TAG));
                index++;
            }
        } else if (obj instanceof Map) {
            list.add(eventType.setEvent(EventType.defaultEventData(obj, tableName),index,TAG));
        } else {
            throw new CoreException("");
        }
        return list;
    }
}

package io.tapdata.js.connector.server.sender;

import io.tapdata.common.support.APISender;
import io.tapdata.entity.logger.TapLogger;
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
    public void send(Object data, Object offsetState) {
        this.send(data, offsetState, APISender.EVENT_INSERT);
    }

    @Override
    public void send(Object data, Object offsetState, String eventType) {
        if (Objects.isNull(core)) {
            TapLogger.warn(TAG, "ScriptCore can not be null or not be empty.");
            return;
        }
        data = LoadJavaScripter.covertData(data);
        core.push(this.covertList(data), eventType, offsetState);
    }

    Map<Integer, Object> cacheAgoData = new ConcurrentHashMap<>();

    @Override
    public void send(Object data, Object offsetState, boolean cacheAgoRecord) {
        if (!cacheAgoRecord) this.send(data, offsetState);
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
        this.send(data, offsetState);
    }

    private Integer hashCode(Object obj) {
        if (Objects.isNull(obj)) return -1;
        return toJson(obj).hashCode();
    }
}

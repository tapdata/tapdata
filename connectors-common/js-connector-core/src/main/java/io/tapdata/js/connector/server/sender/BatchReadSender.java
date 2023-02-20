package io.tapdata.js.connector.server.sender;

import io.tapdata.common.support.APISender;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.base.EventType;
import io.tapdata.js.connector.base.ScriptCore;

import java.util.*;

public class BatchReadSender implements APISender {
    private static final String TAG = BatchReadSender.class.getSimpleName();

    private ScriptCore core;

    public BatchReadSender core(ScriptCore core) {
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
        core.push(this.covertList(data, tableName), eventType, new HashMap<>());
    }

    @Override
    public void send(Object data, String tableName, boolean cacheAgoRecord) {
        this.send(data, tableName);
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
            throw new CoreException("");
        }
        return list;
    }
}

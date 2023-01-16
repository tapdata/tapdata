package io.tapdata.js.connector.server.sender;

import io.tapdata.common.support.APISender;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.base.ScriptCore;

import java.util.Objects;

public class BatchReadSender implements APISender {
    private static final String TAG = BatchReadSender.class.getSimpleName();

    private ScriptCore core;

    public BatchReadSender core(ScriptCore core) {
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
        core.push(this.covertList(data), eventType, offsetState);
    }

    @Override
    public void send(Object data, Object offsetState, boolean cacheAgoRecord) {
        this.send(data, offsetState);
    }
}

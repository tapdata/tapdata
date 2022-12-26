package io.tapdata.js.connector.server.sender;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.base.Core;
import io.tapdata.js.connector.base.ScriptCore;
import io.tapdata.pdk.apis.javascript.APISender;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StreamReadSender implements APISender {
    private static final String TAG = StreamReadSender.class.getSimpleName();

    ScriptCore core ;
    public StreamReadSender core(ScriptCore core){
        this.core = core;
        return this;
    }
    @Override
    public void send(List<Object> data, boolean hasNext, Object offsetState) {
        if(Objects.isNull(core)){
            TapLogger.warn(TAG,"ScriptCore can not be null or not be empty.");
            return;
        }
        core.push(data, Core.MESSAGE_OPERATION_INSERT,offsetState);
    }
}

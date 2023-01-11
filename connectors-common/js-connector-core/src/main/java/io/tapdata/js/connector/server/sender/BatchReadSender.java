package io.tapdata.js.connector.server.sender;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.base.Core;
import io.tapdata.js.connector.base.ScriptCore;
import io.tapdata.common.support.APISender;
import io.tapdata.js.connector.iengine.LoadJavaScripter;

import java.util.*;

public class BatchReadSender implements APISender {
    private static final String TAG = BatchReadSender.class.getSimpleName();

    ScriptCore core ;
    public BatchReadSender core(ScriptCore core){
        this.core = core;
        return this;
    }

    @Override
    public void send(Object data, Object offsetState) {
        if(Objects.isNull(core)){
            TapLogger.warn(TAG,"ScriptCore can not be null or not be empty.");
            return ;
        }
        core.push(this.covertList(data), Core.MESSAGE_OPERATION_INSERT,offsetState);
    }

    @Override
    public void send(Object data, Object offsetState, boolean cacheAgoRecord) {
        this.send(data , offsetState);
    }
}

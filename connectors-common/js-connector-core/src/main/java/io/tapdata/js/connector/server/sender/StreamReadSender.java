package io.tapdata.js.connector.server.sender;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.base.Core;
import io.tapdata.js.connector.base.ScriptCore;
import io.tapdata.common.support.APISender;
import io.tapdata.js.connector.iengine.LoadJavaScripter;

import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.toJson;

public class StreamReadSender implements APISender {
    private static final String TAG = StreamReadSender.class.getSimpleName();

    ScriptCore core ;
    public StreamReadSender core(ScriptCore core){
        this.core = core;
        return this;
    }
    @Override
    public void send(Object data, Object offsetState) {
        if(Objects.isNull(core)){
            TapLogger.warn(TAG,"ScriptCore can not be null or not be empty.");
            return;
        }
        data = LoadJavaScripter.covertData(data);
        core.push(this.covertList(data), Core.MESSAGE_OPERATION_INSERT,offsetState);
    }

    Map<Integer,Object> cacheAgoData = new HashMap<>();
    @Override
    public void send(Object data, Object offsetState, boolean cacheAgoRecord) {
//        if (!cacheAgoRecord) this.send(data,offsetState);
//        List<Object> newData = data.stream().filter(dataItem-> {
//                Integer hashCode = this.hashCode(dataItem);
//                return -1 != hashCode && Objects.isNull(cacheAgoData.get(hashCode));
//            }
//        ).collect(Collectors.toList());
//        cacheAgoData = new HashMap<>();
//        newData.stream().filter(Objects::nonNull).forEach(item-> cacheAgoData.put(this.hashCode(item),item));
        this.send(data,offsetState);
    }

    private Integer hashCode(Object obj){
        if (Objects.isNull(obj)) return -1;
        return toJson(obj).hashCode();
    }
}

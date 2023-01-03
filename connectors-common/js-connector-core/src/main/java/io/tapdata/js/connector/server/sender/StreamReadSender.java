package io.tapdata.js.connector.server.sender;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.base.Core;
import io.tapdata.js.connector.base.ScriptCore;
import io.tapdata.common.support.APISender;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    public boolean send(List<Object> data, boolean hasNext, Object offsetState) {
        if(Objects.isNull(core)){
            TapLogger.warn(TAG,"ScriptCore can not be null or not be empty.");
            return false;
        }
        core.push(data, Core.MESSAGE_OPERATION_INSERT,offsetState);
        return hasNext;
    }

    Map<Integer,Object> cacheAgoData = new HashMap<>();
    @Override
    public boolean send(List<Object> data, boolean hasNext, Object offsetState, boolean cacheAgoRecord) {
        if (!cacheAgoRecord) this.send(data,hasNext,offsetState);
        List<Object> newData = data.stream().filter(dataItem-> {
                Integer hashCode = this.hashCode(dataItem);
                return -1 != hashCode && Objects.isNull(cacheAgoData.get(hashCode));
            }
        ).collect(Collectors.toList());
        cacheAgoData = new HashMap<>();
        newData.stream().filter(Objects::nonNull).forEach(item-> cacheAgoData.put(this.hashCode(item),item));
        return this.send(newData,hasNext,offsetState);
    }

    private Integer hashCode(Object obj){
        if (Objects.isNull(obj)) return -1;
        return toJson(obj).hashCode();
    }
}

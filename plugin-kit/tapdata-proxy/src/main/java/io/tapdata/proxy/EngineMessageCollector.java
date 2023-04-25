package io.tapdata.proxy;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.entity.message.EngineMessage;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class EngineMessageCollector implements MemoryFetcher {
    private final Map<String, EngineMessage> invokeIdEngineMessageMap = new ConcurrentHashMap<>();
    private final LongAdder counter = new LongAdder();
    private final LongAdder totalTakes = new LongAdder();

    private final LongAdder requestBytes = new LongAdder();
    private final LongAdder responseBytes = new LongAdder();
    private Long lastErrorTime;
    private Throwable lastError;
    public EngineMessageCollector lastError(Throwable lastError) {
        this.lastError = lastError;
        lastErrorTime = System.currentTimeMillis();
        return this;
    }

    public EngineMessageCollector() {

    }

    public Map<String, EngineMessage> getInvokeIdEngineMessageMap() {
        return invokeIdEngineMessageMap;
    }

    public LongAdder getCounter() {
        return counter;
    }

    public LongAdder getTotalTakes() {
        return totalTakes;
    }

    public LongAdder getRequestBytes() {
        return requestBytes;
    }

    public LongAdder getResponseBytes() {
        return responseBytes;
    }

    public Throwable getLastError() {
        return lastError;
    }

    public void setLastError(Throwable lastError) {
        this.lastError = lastError;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
                .kv("counter", counter.longValue())
                .kv("totalTakes", totalTakes.longValue())
                .kv("requestBytes", requestBytes.longValue())
                .kv("responseBytes", responseBytes.longValue())
                .kv("lastError", lastError != null ? lastError.getClass().getSimpleName() + ": " + lastError.getMessage() : null)
                .kv("lastErrorTime", lastErrorTime != null ? new Date(lastErrorTime).toString() : null)
                ;

        boolean detailed = true;
        if(memoryLevel != null && memoryLevel.equalsIgnoreCase(MemoryFetcher.MEMORY_LEVEL_SUMMARY)) {
            detailed = false;
        }
        if(detailed) {
            DataMap invokerMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
            dataMap.kv("invokeIdEngineMessageMap", invokerMap);
            for(Map.Entry<String, EngineMessage> entry : invokeIdEngineMessageMap.entrySet()) {
                if(entry.getValue() != null) {
                    Long createTime = entry.getValue().getCreateTime();
                    if(createTime == null)
                        createTime = 0L;
                    EngineMessage message = entry.getValue();
                    invokerMap.kv(entry.getKey(), map(
                                    entry("runningAt", CommonUtils.dateString(new Date(createTime))),
                                    entry("usedMilliseconds", System.currentTimeMillis() - createTime),
                                    entry("engineMessage", InstanceFactory.instance(JsonParser.class).toJson(entry.getValue())),
                                    entry("requestBytes", message.getRequestBytes()),
                                    entry("otherTMIpPort", message.getOtherTMIpPort()),
                                    entry("internalRequest", message.getInternalRequest())
                            ));
                }
            }
        } else {
            dataMap.kv("totalInvocation", invokeIdEngineMessageMap.size());
        }

        return dataMap;
    }
}

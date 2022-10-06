package io.tapdata.pdk.core.monitor;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class InvocationCollector implements MemoryFetcher {
    private PDKMethod pdkMethod;
    private Map<String, Long> invokeIdTimeMap = new ConcurrentHashMap<>();
    private LongAdder counter = new LongAdder();
    private LongAdder totalTakes = new LongAdder();

    public InvocationCollector(PDKMethod method) {
        pdkMethod = method;
    }

    public PDKMethod getPdkMethod() {
        return pdkMethod;
    }

    public void setPdkMethod(PDKMethod pdkMethod) {
        this.pdkMethod = pdkMethod;
    }

    public Map<String, Long> getInvokeIdTimeMap() {
        return invokeIdTimeMap;
    }

    public void setInvokeIdTimeMap(Map<String, Long> invokeIdTimeMap) {
        this.invokeIdTimeMap = invokeIdTimeMap;
    }

    public LongAdder getCounter() {
        return counter;
    }

    public void setCounter(LongAdder counter) {
        this.counter = counter;
    }

    public LongAdder getTotalTakes() {
        return totalTakes;
    }

    public void setTotalTakes(LongAdder totalTakes) {
        this.totalTakes = totalTakes;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
                .kv("counter", counter.longValue())
                .kv("totalTakes", totalTakes.longValue())
                ;

        boolean detailed = true;
        if(memoryLevel != null && memoryLevel.equalsIgnoreCase(MemoryFetcher.MEMORY_LEVEL_SUMMARY)) {
            detailed = false;
        }
        if(detailed) {
            DataMap invokerMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
            dataMap.kv("invokeIdTimeMap", invokerMap);
            for(Map.Entry<String, Long> entry : invokeIdTimeMap.entrySet()) {
                if(entry.getValue() != null)
                    invokerMap.kv(entry.getKey(), map(
                                    entry("runningAt", CommonUtils.dateString(new Date(entry.getValue()))),
                                    entry("usedMilliseconds", System.currentTimeMillis() - entry.getValue())
                            ));
            }
        } else {
            dataMap.kv("totalInvocation", invokeIdTimeMap.size());
        }

        return dataMap;
    }
}

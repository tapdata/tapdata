package io.tapdata.pdk.core.monitor;

import io.tapdata.entity.utils.ParagraphFormatter;
import io.tapdata.pdk.core.memory.MemoryFetcher;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class InvocationCollector {
    private PDKMethod pdkMethod;
    private Map<String, Long> invokeIdTimeMap = new ConcurrentHashMap<>();
    private LongAdder counter = new LongAdder();
    private LongAdder totalTakes = new LongAdder();

    public String toMemoryString(String memoryLevel) {
        return toMemoryString(memoryLevel, 1);
    }
    public String toMemoryString(String memoryLevel, int indentation) {
        ParagraphFormatter paragraphFormatter = new ParagraphFormatter(InvocationCollector.class.getSimpleName(), indentation)
                .addRow("Counter", counter.longValue())
                .addRow("TotalTakes", totalTakes.longValue())
                ;

        boolean detailed = true;
        if(memoryLevel != null && memoryLevel.equalsIgnoreCase(MemoryFetcher.MEMORY_LEVEL_SUMMARY)) {
            detailed = false;
        }
        if(detailed) {
            for(Map.Entry<String, Long> entry : invokeIdTimeMap.entrySet()) {
                if(entry.getValue() != null)
                    paragraphFormatter.addRow("InvokeId", entry.getKey(),
                            "RunningAt", CommonUtils.dateString(new Date(entry.getValue())),
                            "UsedMilliseconds", System.currentTimeMillis() - entry.getValue());
            }
        } else {
            paragraphFormatter.addRow("TotalInvocation", invokeIdTimeMap.size());
        }

        return paragraphFormatter.toString();
    }
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
}

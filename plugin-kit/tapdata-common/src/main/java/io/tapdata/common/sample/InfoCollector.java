package io.tapdata.common.sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Dexter
 */
public class InfoCollector {
    private static final Logger logger = LoggerFactory.getLogger(InfoCollector.class.getSimpleName());

    private String name;

    private final Map<String, Info<?>> idInfoMap = new ConcurrentHashMap<>();

    final Map<String, Object> result = new HashMap<>();

    private Map<String, String> tags = new ConcurrentHashMap<>();

    public InfoCollector withTag(String key, String value) {
        if(key != null && value != null)
            tags.put(key, value);
        return this;
    }
    public InfoCollector withTags(Map<String, String> tags) {
        if(tags != null)
            this.tags.putAll(tags);
        return this;
    }

    public void clearTags() {
        tags.clear();
    }

    public String deleteTag(String key) {
        return tags.remove(key);
    }

    public Map<String, String> tags() {
        return tags;
    }

    public InfoCollector() {

    }

    public InfoCollector withName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    void collect() {
        try {
            result.clear();
            for(Map.Entry<String, Info<?>> entry : idInfoMap.entrySet()) {
                try {
                    long time = System.currentTimeMillis();
                    result.put(entry.getKey(), entry.getValue().value());
                    long takes = System.currentTimeMillis() - time;
                    if(takes > 10) {
                        logger.debug("Info {} execute more than 10 milliseconds, {}", entry.getValue().getClass().getSimpleName(), takes);
                    }
                } catch(Throwable throwable) {
                    throwable.printStackTrace();
                    logger.error("Info {} sample failed, {}", entry.getValue().getClass().getSimpleName(), throwable.getMessage());
                }
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            logger.error("PointExecutor calculate result failed, {}", throwable.getMessage());
        }
    }

    /**
     * Add a self configured info into InfoCollector.
     */
    public void addInfo(String id, Info<?> info) {
        idInfoMap.putIfAbsent(id, info);
    }


    public static void main(String... args) {
    }
}

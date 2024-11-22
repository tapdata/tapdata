package io.tapdata.observable.logging.debug;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.IteratorUtils;
import org.ehcache.Cache;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/19 14:55
 */
public class DataCache {

    @Getter
    @Setter
    private String taskId;
    @Getter
    @Setter
    private Integer maxTotalEntries;
    private AtomicInteger counter;

    private Cache<String, MonitoringLogsDto> cache;

    public DataCache(String taskId, Integer maxTotalEntries, Cache<String, MonitoringLogsDto> cache) {
        this.taskId = taskId;
        this.maxTotalEntries = maxTotalEntries;
        this.cache = cache;
        counter = new AtomicInteger(0);
    }

    public void put(MonitoringLogsDto monitoringLogsDto) {
        if (counter.incrementAndGet() <= maxTotalEntries) {
            Optional.ofNullable(cache).ifPresent(c ->
                    c.put(System.currentTimeMillis() + "", monitoringLogsDto));
        }
    }

    public List<MonitoringLogsDto> getAndRemoveAll() {
        List<MonitoringLogsDto> result = new ArrayList<>();
        if (cache != null) {
            IteratorUtils.asIterable(cache.iterator()).forEach(v -> result.add(v.getValue()));
            cache.clear();
        }
        counter.set(0);
        return result;
    }

    public void destroy() {
        cache = null;
    }

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("counter", counter.get());
        status.put("maxTotalEntries", maxTotalEntries);
        if (cache == null) {
            status.put("cache", "Cache is null");
        } else {
            AtomicInteger count = new AtomicInteger(0);
            cache.forEach(t -> count.incrementAndGet());
            status.put("cacheCount", count.get());
        }
        return status;
    }
}

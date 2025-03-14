package io.tapdata.observable.logging.debug;

import com.alibaba.fastjson.JSON;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.observable.logging.ObsLoggerFactory;
import lombok.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.ehcache.Cache;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/19 14:55
 */
public class DataCache {

    public static Map<String, Object> emptyResult = new HashMap<>();
    public static int processTimeout = 120000;  // 2 minutes
    static {
        emptyResult.put("hasMore", false);
        emptyResult.put("data", emptyList());
    }

    @Getter
    @Setter
    private String taskId;
    @Getter
    @Setter
    private Long maxTotalEntries;
    private Lock lock = new ReentrantLock();

    private Cache<String, CacheItem> cache;

    public DataCache(String taskId, Long maxTotalEntries, Cache<String, CacheItem> cache) {
        this.taskId = taskId;
        this.maxTotalEntries = maxTotalEntries;
        this.cache = cache;
    }

    public void put(MonitoringLogsDto monitoringLogsDto) {
        if (monitoringLogsDto == null)
            return;
        if (maxTotalEntries == null || getCacheSize() < maxTotalEntries) {
            String eid = getEventId(monitoringLogsDto.getLogTags());
            if (eid == null)
                return;
            Optional.ofNullable(cache).ifPresent(c -> {
                lock.lock();
                try {
                    CacheItem cacheItem;
                    boolean exists = c.containsKey(eid);
                    if (exists){
                        cacheItem = c.get(eid);
                        cacheItem.put(monitoringLogsDto.getNodeId(), monitoringLogsDto);
                        c.replace(eid, cacheItem);
                    } else {
                        cacheItem = CacheItem.builder()
                                .id(eid)
                                .ts(monitoringLogsDto.getTimestamp())
                                .expireAt(System.currentTimeMillis() + processTimeout)
                                .type(getEventType(monitoringLogsDto.getLogTags()))
                                .build();
                        cacheItem.put(monitoringLogsDto.getNodeId(), monitoringLogsDto);
                        c.put(eid, cacheItem);
                    }
                } finally {
                    lock.unlock();
                }
            });
        }
    }

    private String getEventId(List<String> logTags) {
        if (CollectionUtils.isEmpty(logTags))
            return null;
        return logTags.stream().filter(s -> s.startsWith("eid=")).findFirst().orElse(null);
    }

    private String getEventType(List<String> logTags) {
        if (CollectionUtils.isEmpty(logTags))
            return null;
        return logTags.stream().filter(s -> s.startsWith("type=")).findFirst().orElse(null);
    }

    public Map<String, Object> searchAndRemove(Integer count, String query) {

        if (cache == null) {
            return emptyMap();
        }
        List<CacheItem> dataList = new ArrayList<>();
        Iterator<Cache.Entry<String, CacheItem>> iterator = cache.iterator();
        boolean hasQuery = StringUtils.isNotBlank(query);
        boolean getData;
        if (count == null)
            count = 10;
        int skipUncompleted = 0;
        while (count > 0 && iterator.hasNext()) {
            Cache.Entry<String, CacheItem> entry = iterator.next();
            CacheItem cacheItem = entry.getValue();

            if (!cacheItem.processCompleted && !cacheItem.isExpired()) {
                skipUncompleted++;
                continue;
            }

            if (hasQuery) {
                Optional<Map<String, Object>> optional = cacheItem.getData().values().stream().flatMap(m -> m.getData().stream())
                        .filter(d -> match(d, query)).findFirst();
                getData = optional.isPresent();
            } else
                getData = true;
            if (getData) {
                count--;
                dataList.add(cacheItem);
                iterator.remove();
            }
        }

        if (dataList.isEmpty()) return emptyResult;
        Map<String, Object> result = new HashMap<>();
        result.put("skipUncompleted", skipUncompleted);
        result.put("data", dataList);
        result.put("hasMore", iterator.hasNext());

        ObsLoggerFactory.getInstance().onFetchCacheData(taskId);

        return result;
    }

    public void processCompleted(String eventId) {
        if (StringUtils.isBlank(eventId))
            return;
        if (!eventId.startsWith("eid=")) {
            eventId = "eid=" + eventId;
        }
        try {
            if (cache != null && cache.containsKey(eventId)) {
                CacheItem item = cache.get(eventId);
                if (item != null)
                    item.setProcessCompleted(true);
                cache.replace(eventId, item);
            }
        } catch (Exception e){}
    }

    public void destroy() {
        Optional.ofNullable(cache).ifPresent(Cache::clear);
        cache = null;
    }

    public long getCacheSize() {
        if (cache != null) {
            AtomicLong count = new AtomicLong(0);
            cache.forEach(t -> count.incrementAndGet());
            return count.longValue();
        }
        return 0;
    }

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("maxTotalEntries", maxTotalEntries);
        if (cache == null) {
            status.put("cache", "Cache is null");
        } else {
            status.put("cacheCount", getCacheSize());
        }
        return status;
    }

    public static void markCatchEventWhenMatched(TapdataEvent event, String taskId, String query) {
        if (query == null) {
            event.setCatchMe(true);
            return;
        }
        DataCache dataCache = DataCacheFactory.getInstance().getDataCache(taskId);
        boolean match = false;
        TapEvent e = event.getTapEvent();
        if (e instanceof TapInsertRecordEvent) {
            match = dataCache.match(((TapInsertRecordEvent)e).getAfter(), query);
        } else if (e instanceof TapUpdateRecordEvent) {
            match = dataCache.match(((TapUpdateRecordEvent)e).getBefore(), query);
            if (!match)
                match = dataCache.match(((TapUpdateRecordEvent)e).getAfter(), query);
        } else if (e instanceof TapDeleteRecordEvent) {
            match = dataCache.match(((TapDeleteRecordEvent)e).getBefore(), query);
        }
        event.setCatchMe(match);
    }

    private boolean match(Map<String, Object> data, String query) {
        return Optional.ofNullable(JSON.toJSON(data, DataCacheFactory.dataSerializeConfig))
                .map(Object::toString)
                .map(s -> s.contains(query)).orElse(false);
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CacheItem {
        private String id;
        private String type;
        private Long ts;
        private Map<String, MonitoringLogsDto> data;
        private boolean processCompleted = false;

        private long expireAt;

        public boolean isExpired() {
            return System.currentTimeMillis() > expireAt;
        }

        public void put(String key, MonitoringLogsDto v) {
            if (StringUtils.isBlank(key))
                return ;
            if (data == null) {
                data = new HashMap<>();
            }
            data.put(key, v);
        }
    }
}

package io.tapdata.common.sample;

import com.google.common.collect.ImmutableMap;
import io.tapdata.common.executor.ExecutorsManager;
import io.tapdata.common.sample.request.*;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.ResetCounterSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public class CollectorFactory {
    private static final Logger logger = LoggerFactory.getLogger(CollectorFactory.class.getSimpleName());

    private static final int periodSeconds = 5;

    private static final Map<String, CollectorFactory> INSTANCES = new ConcurrentHashMap<>();

    public static CollectorFactory getInstance(String key) {
        INSTANCES.putIfAbsent(key, new CollectorFactory());
        return INSTANCES.get(key);
    }

    public static CollectorFactory getInstance() {
        return getInstance("v1");
    }

    /**
     * sample collectors are time series base, data are organized by timeline.
     */
    private final Map<String, SampleCollector> crcSampleCollector = new ConcurrentHashMap<>();

    /**
     * statistic collectors are not time series base, it means that the same
     * tagged value will be overwritten.
     */
    private final Map<String, SampleCollector> crcStatisticCollector = new ConcurrentHashMap<>();

    /**
     * disposable collectors are dropped after the offering the samples, the
     * same tagged value will also be overwritten.
     */
    private final Map<String, InfoCollector> crcDisposableCollector = new ConcurrentHashMap<>();

    /**
     * cached bulk Request
     */
    private final List<BulkRequest> cachedBulkRequest= new ArrayList<>();
    private final Map<String, Integer> cachedBulkRequestIdx = new ConcurrentHashMap<>();
    private final ReentrantLock cachedBulkRequestLock = new ReentrantLock();

    private final Map<String, Map<String, SampleCollector>> sampleCollectorGroup = new HashMap<>();
    private final Map<String, Map<String, SampleCollector>> statisticCollectorGroup = new HashMap<>();
    private final ReentrantLock collectorGroupLock = new ReentrantLock();

    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    public CollectorFactory() {
        scheduledExecutorService = ExecutorsManager.getInstance().newSingleThreadScheduledExecutor(CollectorFactory.class.getSimpleName());
    }

    public void start(BulkReporter bulkReporter, InfoReporter infoReporter) {
        if(isStarted.compareAndSet(false, true)) {
            if (bulkReporter == null || infoReporter == null) {
                throw new IllegalArgumentException("BulkReporter and InfoReporter should be provided to report " +
                        "collected samples;");
            }
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                if (!crcDisposableCollector.isEmpty()) {
                    // add a backup to avoid network error when report collected info samples
                    List<String> succeed = new ArrayList<>();
                    for(String key : crcDisposableCollector.keySet()) {
                        InfoCollector collector = crcDisposableCollector.remove(key);
                        collector.collect();
                        try {
                            // only report the collected info samples when samples are not empty
                            if (!collector.result.isEmpty()) {
                                infoReporter.execute(collector.result, collector.tags());
                            }
                            succeed.add(key);
                        } catch(Throwable throwable) {
                            throwable.printStackTrace();
                            logger.error("Report info failed, " + throwable.getMessage());
                        }
                    }
                    // drop the successfully processed collector
                    for(String key : succeed) {
                        crcDisposableCollector.remove(key);
                    }
                }

                // upload the cached bulkRequest
                cachedBulkRequestLock.lock();
                try {
                    for (BulkRequest bulkRequest : cachedBulkRequest) {
                        try {
                            bulkReporter.execute(bulkRequest);
                        } catch(Throwable throwable) {
                            throwable.printStackTrace();
                            logger.error("Report bulkSampleRequest failed, " + throwable.getMessage());
                        }
                    }
                } finally {
                    // clear cache
                    cachedBulkRequest.clear();
                    cachedBulkRequestIdx.clear();
                    cachedBulkRequestLock.unlock();
                }

                Date date = new Date();
                BulkRequest bulkRequest = new BulkRequest();

                // process the statistic collectors
                for(SampleCollector collector : crcStatisticCollector.values()) {
                    StatisticRequest statisticRequest = wrapStatisticRequest(collector, date, false);
                    if (statisticRequest == null) {
                        continue;
                    }
                    bulkRequest.addStatisticRequest(statisticRequest);
                }


                // process the sample collectors
                for(SampleCollector collector : crcSampleCollector.values()) {
                    SampleRequest sampleRequest = wrapSampleRequest(collector, date, false);
                    if (sampleRequest == null) {
                        continue;
                    }
                    bulkRequest.addSampleRequest(sampleRequest);
                }

                if (bulkRequest.getStatistics().size() ==0 && bulkRequest.getSamples().size() == 0) {
                    return;
                }

                try {
                    bulkReporter.execute(bulkRequest);
                } catch(Throwable throwable) {
                    throwable.printStackTrace();
                    logger.error("Report bulkSampleRequest failed, " + throwable.getMessage());
                }
            }, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        }
    }

    public void start(BulkReporter bulkReporter) {
        if(isStarted.compareAndSet(false, true)) {
            if (bulkReporter == null) {
                throw new IllegalArgumentException("BulkReporter should be provided to report collected samples;");
            }
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                // upload the cached bulkRequest
                cachedBulkRequestLock.lock();
                try {
                    for (BulkRequest bulkRequest : cachedBulkRequest) {
                        try {
                            bulkReporter.execute(bulkRequest);
                        } catch(Throwable throwable) {
                            throwable.printStackTrace();
                            logger.error("Report bulkSampleRequest failed, " + throwable.getMessage());
                        }
                    }
                } finally {
                    // clear cache
                    cachedBulkRequest.clear();
                    cachedBulkRequestIdx.clear();
                    cachedBulkRequestLock.unlock();
                }

                Date date = new Date();
                BulkRequest bulkRequest = new BulkRequest();

                // process the statistic collectors
                for(SampleCollector collector : crcStatisticCollector.values()) {
                    StatisticRequest statisticRequest = wrapStatisticRequest(collector, date, false);
                    if (statisticRequest == null) {
                        continue;
                    }
                    bulkRequest.addStatisticRequest(statisticRequest);
                }


                // process the sample collectors
                for(SampleCollector collector : crcSampleCollector.values()) {
                    SampleRequest sampleRequest = wrapSampleRequest(collector, date, false);
                    if (sampleRequest == null) {
                        continue;
                    }
                    bulkRequest.addSampleRequest(sampleRequest);
                }

                if (bulkRequest.getStatistics().size() ==0 && bulkRequest.getSamples().size() == 0) {
                    return;
                }

                try {
                    bulkReporter.execute(bulkRequest);
                } catch(Throwable throwable) {
                    throwable.printStackTrace();
                    logger.error("Report bulkSampleRequest failed, " + throwable.getMessage());
                }
            }, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        }
    }

    private StatisticRequest wrapStatisticRequest(SampleCollector collector, Date date, boolean cache) {
        Map<String, Number> result = collector.calculateInPeriod();
        if (result.isEmpty()) {
            return null;
        }
        long count = result.entrySet().stream().filter(map -> Objects.isNull(map.getValue())).count();
        if (count == result.size()) {
            return null;
        }

        StatisticRequest statisticRequest = new StatisticRequest();
        statisticRequest.setTags(collector.tags());
        Statistic statistic = new Statistic();
        statistic.setDate(date);
        if (cache) {
            Map<String, Number> value = new HashMap<>();
            for (Map.Entry<String, Number> entry : result.entrySet()) {
                if (entry.getValue() != null) {
                    value.put(entry.getKey(), entry.getValue());
                }
            }
            statistic.setValues(value);
        } else {
            statistic.setValues(result);
        }
        statistic.setIncFields(collector.IncrSamples());
        statisticRequest.setStatistic(statistic);
        return statisticRequest;
    }

    private SampleRequest wrapSampleRequest(SampleCollector collector, Date date, boolean cache) {
        Map<String, Number> result = collector.calculateInPeriod();
        if (result.isEmpty()) {
            return null;
        }
        long count = result.entrySet().stream().filter(map -> Objects.isNull(map.getValue())).count();
        if (count == result.size()) {
            return null;
        }

        SampleRequest sampleRequest = new SampleRequest();
        sampleRequest.setTags(collector.tags());
        Sample sample = new Sample();
        sample.setDate(date);
        if (cache) {
            Map<String, Number> value = new HashMap<>();
            for (Map.Entry<String, Number> entry : result.entrySet()) {
                if (entry.getValue() != null) {
                    value.put(entry.getKey(), entry.getValue());
                }
            }
            sample.setVs(value);
        } else {
            sample.setVs(result);
        }
        sampleRequest.setSample(sample);
        return sampleRequest;
    }

    /**
     * 在 init 的时候 或者 destroy 的时候，将 collector 中的数据暂存下来，等到下一次上报的时候，在上报该时间点数据前上报上去。
     */
    public void recordCurrentValueByTag(Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return;
        }
        cachedBulkRequestLock.lock();
        try {
            Date now = new Date();
            String crc = generateMapKey(tags);
            SampleCollector sampleCollector = crcSampleCollector.getOrDefault(crc, null);
            SampleCollector statisticsCollector = crcStatisticCollector.getOrDefault(crc, null);

            Integer idx = null;
            for(int i=0; i < 4; i++) {
                if (!cachedBulkRequestIdx.containsKey(crc)) {
                    idx = i;
                    cachedBulkRequestIdx.put(crc, idx);
                    break;
                }
                crc = generateMapKey(tags, i);
            }
            if (idx == null) {
                throw new RuntimeException("failed to get the appropriate index to cache the node sample value, " +
                        "must be a bug or the report interval is too long. ");
            }

            if (cachedBulkRequest.size() <= idx) {
                cachedBulkRequest.add(new BulkRequest());
            }
            BulkRequest bulkRequest = cachedBulkRequest.get(idx);
            if (sampleCollector != null) {
                SampleRequest sampleRequest = wrapSampleRequest(sampleCollector, now, true);
                if (sampleRequest != null) {
                    bulkRequest.getSamples().add(sampleRequest);
                }
            }
            if (statisticsCollector != null) {
                StatisticRequest statisticRequest = wrapStatisticRequest(statisticsCollector, now, true);
                if (statisticRequest != null) {
                    bulkRequest.getStatistics().add(statisticRequest);
                }
            }
        } finally {
            cachedBulkRequestLock.unlock();
        }
    }

    /**
     * 通过不同的Tags获取对应SampleCollector。 这个get性能不好， 调用层最好能缓存返回的SampleCollector
     * 主要是需要基于Tags生成唯一ID
     *
     * @param name
     * @param tags
     * @return
     */
    public SampleCollector getSampleCollectorByTags(String name, Map<String, String> tags) {
        String crc = generateMapKey(tags);
        return crcSampleCollector.computeIfAbsent(crc, s -> new SampleCollector()
                .withName(name)
                .withTags(tags));
    }

    public SampleCollector removeSampleCollectorByTags(Map<String, String> tags) {
        String crc = generateMapKey(tags);
        SampleCollector sampleCollector = crcSampleCollector.remove(crc);
        if(sampleCollector != null) {
            sampleCollector.stop();
        }
        return sampleCollector;
    }

    public SampleCollector getStatisticCollectorByTags(String name, Map<String, String> tags) {
        String crc = generateMapKey(tags);
        return crcStatisticCollector.computeIfAbsent(crc, s -> new SampleCollector()
                .withName(name)
                .withTags(tags));
    }

    public SampleCollector removeStatisticCollectorByTags(Map<String, String> tags) {
        String crc = generateMapKey(tags);
        SampleCollector statisticCollector = crcStatisticCollector.remove(crc);
        if(statisticCollector != null) {
            statisticCollector.stop();
        }
        return statisticCollector;
    }

    @Deprecated
    public void registerSampleCollectorToGroup(String groupId, SampleCollector collector) {
        if (groupId == null || collector == null) {
            return;
        }
        collectorGroupLock.lock();
        try {
            String crc = generateMapKey(collector.getTags());
            sampleCollectorGroup.computeIfAbsent(groupId, s -> new HashMap<>());
            Map<String, SampleCollector> crcMap = sampleCollectorGroup.get(groupId);
            crcMap.putIfAbsent(crc, collector);
        } finally {
            collectorGroupLock.unlock();
        }
    }

    @Deprecated
    public void unregisterSampleCollectorFromGroup(String groupId, SampleCollector collector) {
        if (groupId == null || collector == null) {
            return;
        }
        collectorGroupLock.lock();
        try {
            if (!sampleCollectorGroup.containsKey(groupId)) {
                return;
            }
            String crc = generateMapKey(collector.getTags());
            Map<String, SampleCollector> crcMap = sampleCollectorGroup.get(groupId);
            if (!crcMap.containsKey(crc)) {
                return;
            }
            crcMap.get(crc).pendingCollector();

            // if all the registered  collector is in pending, stop all collectors
            for(SampleCollector item : crcMap.values()) {
                if (!item.isPending()) {
                    return;
                }
            }
            for(SampleCollector item : crcMap.values()) {
                recordCurrentValueByTag(item.tags());
                item.stop();
                removeSampleCollectorByTags(item.tags());
            }
            sampleCollectorGroup.remove(groupId);
        } finally {
            collectorGroupLock.unlock();
        }

    }

    @Deprecated
    public void registerStatisticCollectorToGroup(String groupId, SampleCollector collector) {
        if (groupId == null || collector == null) {
            return;
        }
        collectorGroupLock.lock();
        try {
            String crc = generateMapKey(collector.getTags());
            statisticCollectorGroup.computeIfAbsent(groupId, s -> new HashMap<>());
            Map<String, SampleCollector> crcMap = statisticCollectorGroup.get(groupId);
            crcMap.putIfAbsent(crc, collector);
        } finally {
            collectorGroupLock.unlock();
        }
    }

    @Deprecated
    public void unregisterStatisticCollectorFromGroup(String groupId, SampleCollector collector) {
        if (groupId == null || collector == null) {
            return;
        }
        collectorGroupLock.lock();
        try {
            if (!statisticCollectorGroup.containsKey(groupId)) {
                return;
            }
            String crc = generateMapKey(collector.getTags());
            Map<String, SampleCollector> crcMap = statisticCollectorGroup.get(groupId);
            if (!crcMap.containsKey(crc)) {
                return;
            }
            crcMap.get(crc).pendingCollector();

            // if all the registered  collector is in pending, stop all collectors
            for(SampleCollector item : crcMap.values()) {
                if (!item.isPending()) {
                    return;
                }
            }
            for(SampleCollector item : crcMap.values()) {
                recordCurrentValueByTag(item.tags());
                item.stop();
                removeStatisticCollectorByTags(item.tags());
            }
            statisticCollectorGroup.remove(groupId);
        } finally {
            collectorGroupLock.unlock();
        }

    }

    public InfoCollector getDisposableCollectorByTags(String name, Map<String, String> tags) {
        String crc = generateMapKey(tags);
        return crcDisposableCollector.computeIfAbsent(crc, s -> new InfoCollector()
                .withName(name)
                .withTags(tags));
    }

    public InfoCollector removeDisposableCollectorByTags(Map<String, String> tags) {
        String crc = generateMapKey(tags);
        return crcDisposableCollector.remove(crc);
    }

    public String generateMapKey(Map<String, String> map, Integer index) {
        TreeMap<String, String> sortMap = new TreeMap<>(String::compareTo) ;
        sortMap.putAll(map);
        StringBuilder builder = new StringBuilder();
        for(Map.Entry<String, String> entry : sortMap.entrySet()) {
            builder.append(entry.getKey()).append(entry.getValue());
        }
        if (index != null) {
            builder.append(index);
        }
        CRC32 crc32 = new CRC32();
        byte[] bytes=builder.toString().getBytes(StandardCharsets.UTF_8);
        crc32.update(bytes,0,bytes.length);
        long crcValue = crc32.getValue();
        return String.valueOf(crcValue);
    }


    public String generateMapKey(Map<String, String> map) {
        return generateMapKey(map, null);
    }

    public static void main(String... args) {
        Random random = new Random();
        CollectorFactory.getInstance().start(
                (bulkRequest) -> System.out.println(bulkRequest.toString()),
                (pointValues, tags) -> System.out.println(pointValues.toString() + tags.toString())
        );

        InfoCollector disposableCollector = CollectorFactory.getInstance()
                .getDisposableCollectorByTags("disposable", ImmutableMap.of("tag0", "value0"));
        disposableCollector.addInfo("info", () -> "info");
        disposableCollector.addInfo("cpuUsage", () -> 0.3);

        SampleCollector sampleCollector = CollectorFactory.getInstance()
                .getSampleCollectorByTags("sample", ImmutableMap.of("tag1", "value1"));
        AverageSampler averageSampler = sampleCollector.getAverageSampler("httpTakes");
        CounterSampler counterSampler = sampleCollector.getCounterSampler("visitCounter");
        ExecutorsManager.getInstance().newSingleThreadScheduledExecutor("Test").scheduleAtFixedRate(() -> {
            averageSampler.add(random.nextInt(100));
            counterSampler.inc();
        }, 1, 1, TimeUnit.SECONDS);

        SampleCollector statisticCollector = CollectorFactory.getInstance()
                .getStatisticCollectorByTags("statistic", ImmutableMap.of("tag2", "value2"));
        ResetCounterSampler resetCounterStatistic = statisticCollector.getResetCounterSampler("httpTakes");
        CounterSampler counterStatistic = statisticCollector.getCounterSampler("visitCounter");
        ExecutorsManager.getInstance().newSingleThreadScheduledExecutor("Test").scheduleAtFixedRate(() -> {
            resetCounterStatistic.inc(random.nextInt(100));
            counterStatistic.inc();
        }, 1, 1, TimeUnit.SECONDS);
    }
}

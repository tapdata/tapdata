package io.tapdata.common.sample;

import io.tapdata.common.executor.ExecutorsManager;
import io.tapdata.common.sample.process.GcSampler;
import io.tapdata.common.sample.sampler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Generic point collector.
 * 指标点收集器
 * 目前支持4中采集， 平均采集AveragePoint， 计数器采集CounterPoint， 数字采集NumberPoint， 速度采集SpeedPoint
 */
public class SampleCollector {
    private static final Logger logger = LoggerFactory.getLogger(SampleCollector.class.getSimpleName());
    /**
     * 采样周期， 默认5秒
     */
    private int periodSeconds = 5;

    private SampleReporter sampleReporter;
    private String name;
    private CollectorStatus status;
    /**
     * 单线程定时器， 用于做上报回调
     */
    private ScheduledExecutorService scheduleExecutorService;
    private static ScheduledExecutorService globalScheduleExecutorService;
    /**
     * 业务自定义的id对应的采集类型实例
     */
    private final Map<String, Sampler> idSamplerMap = new ConcurrentHashMap<>();

    /**
     * 复用的采样结果容器
     */
//    final Map<String, Number> result = new HashMap<>();

    private Map<String, String> tags = new ConcurrentHashMap<>();
    private ScheduledFuture<?> reportFuture;

    public SampleCollector withTag(String key, String value) {
        if(key != null && value != null)
            tags.put(key, value);
        return this;
    }
    public SampleCollector withTags(Map<String, String> tags) {
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

    public SampleCollector() {

    }
    public SampleCollector(SampleReporter sampleReporter) {
        this.sampleReporter = sampleReporter;
    }

    public SampleCollector withPeriodSeconds(int periodSeconds) {
        this.periodSeconds = periodSeconds;
        return this;
    }

    public SampleCollector withName(String name) {
        this.name = name;
        return this;
    }
    public SampleCollector withScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduleExecutorService = scheduleExecutorService;
        return this;
    }
    /**
     * 需要调用开始之后， 定时采样才会启动
     */
    public SampleCollector start() {
        if(periodSeconds <= 1 )
            throw new IllegalArgumentException("Pointer collector start failed, illegal parameters periodSeconds " + periodSeconds + ", pointerExecutor " + sampleReporter);
        if(sampleReporter != null) {
            synchronized (this) {
                if(scheduleExecutorService == null) {
                    if(globalScheduleExecutorService == null) {
                        globalScheduleExecutorService = ExecutorsManager.getInstance().newSingleThreadScheduledExecutor("PointCollector-" + name);
                    }
                    scheduleExecutorService = globalScheduleExecutorService;
                }
            }
            if(reportFuture != null) {
                reportFuture.cancel(false);
            }
            reportFuture = scheduleExecutorService.scheduleAtFixedRate(this::calculateInPeriod, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        }
        return this;
    }

    Map<String, Number> calculateInPeriod() {
        Map<String, Number> result = new HashMap<>();
        try {
            for(Map.Entry<String, Sampler> entry : idSamplerMap.entrySet()) {
                try {
                    if (entry.getValue() == null) {
                        continue;
                    }
                    long time = System.currentTimeMillis();
                    result.put(entry.getKey(), entry.getValue().value());
                    long takes = System.currentTimeMillis() - time;
                    if(takes > 10) {
                        logger.debug("PointSampler {} key {} execute more than 10 milliseconds, {}", entry.getValue().getClass().getSimpleName(), entry.getKey(), takes);
                    }
                } catch(Throwable throwable) {
                    logger.error("PointSampler {} key {} sample failed, {}", entry.getValue().getClass().getSimpleName(), entry.getKey(), throwable.getMessage(), throwable);
                }
            }
        } catch (Throwable throwable) {
            logger.error("PointExecutor calculate result failed, {}", throwable.getMessage(), throwable);
        }
        if(sampleReporter != null) {
            try {
                sampleReporter.execute(result, tags);
            } catch(Throwable throwable) {
                logger.error("PointExecutor execute failed, {}", throwable.getMessage(), throwable);
            }
        }
        return result;
    }

    public SampleCollector withSampler(String id, Sampler sampler) {
        addSampler(id, sampler);
        return this;
    }

    /**
     *
     * TODO: using cache if there are performance issue
     */
    public List<String> IncrSamples() {
        List<String> values = new ArrayList<>();
        for(String id : idSamplerMap.keySet()) {
            Sampler sampler = idSamplerMap.getOrDefault(id, null);
            if (sampler instanceof ResetCounterSampler) {
                values.add(id);
            }
        }
        return values;
    }

    /**
     * Add a self configured point into PointCollector.
     * @param id
     * @param sampler
     */
    public void addSampler(String id, Sampler sampler) {
        idSamplerMap.putIfAbsent(id, sampler);
    }

    /**
     * 根据业务指定的ID获得计数器采集器
     *
     * @param id
     * @return
     */
    public CounterSampler getCounterSampler(String id) {//没有就新建， 有就返回
        return (CounterSampler) idSamplerMap.computeIfAbsent(id, s -> new CounterSampler());
    }

    /**
     * 根据业务指定的ID获得有初始值的计数器采集器
     *
     * @param id
     * @return
     */
    public CounterSampler getCounterSampler(String id, long initialValue) {//没有就新建， 有就返回，有的话会忽略初始值
        return (CounterSampler) idSamplerMap.computeIfAbsent(id, s -> new CounterSampler(initialValue));
    }

    /**
     * 根据业务指定的ID获得可重置的计数器采集器，当数据被采集之后，计数器会自动自动归零
     *
     * @param id
     * @return
     */
    public ResetCounterSampler getResetCounterSampler(String id) {//没有就新建， 有就返回
        return (ResetCounterSampler) idSamplerMap.computeIfAbsent(id, s -> new ResetCounterSampler());
    }

    /**
     * 根据业务指定的ID获得数字采集器
     *
     * @param id
     * @param numberClass 指定数字的类型， 返回对应泛型的数字采集器
     * @param <T>
     * @return
     */
    public <T extends Number> NumberSampler<T> getNumberCollector(String id, Class<T> numberClass) {
        return (NumberSampler<T>) idSamplerMap.computeIfAbsent(id, s -> new NumberSampler<T>());
    }

    /**
     * 根据业务指定的ID获得带有初始值的数字采集器
     *
     * @param id
     * @param numberClass 指定数字的类型， 返回对应泛型的数字采集器
     * @param <T>
     * @return
     */
    public <T extends Number> NumberSampler<T> getNumberCollector(String id, Class<T> numberClass, T initialValue) {
        return (NumberSampler<T>) idSamplerMap.computeIfAbsent(id, s -> new NumberSampler<T>(initialValue));
    }

    public ResetSampler getResetSampler(String id) {
        return (ResetSampler) idSamplerMap.computeIfAbsent(id, s -> new ResetSampler(null));
    }

    /**
     * 根据业务指定的ID获得平均采集器
     *
     * @param id
     * @return
     */
    public AverageSampler getAverageSampler(String id) {
        return (AverageSampler) idSamplerMap.computeIfAbsent(id, s -> new AverageSampler());
    }

    public WriteCostAvgSampler getWriteCostAvgSampler(String id) {
        return (WriteCostAvgSampler) idSamplerMap.computeIfAbsent(id, s -> new WriteCostAvgSampler());
    }

    /**
     * 根据业务指定的ID获得速度采集器
     *
     * @param id
     * @return
     */
    public SpeedSampler getSpeedSampler(String id) {
        return (SpeedSampler) idSamplerMap.computeIfAbsent(id, s -> new SpeedSampler());
    }


    public static void main(String... args) {
        SampleCollector p = new SampleCollector((pointValues, tags) -> {
            System.out.println("pointValues " + pointValues);
            System.out.println("pointTags " + tags);
            //上报到服务器
        })
                .withName("agentProcess")
                .withTag("a", "b")
                .start();
        p.addSampler("aaa", new GcSampler(GcSampler.GcPointEnum.GC_TIME));
        p.getCounterSampler("aaa234");

        SampleCollector collector = new SampleCollector((pointValues, tags) -> {
            System.out.println("123 pointValues " + pointValues);
            System.out.println("123 pointTags " + tags);
            //上报到服务器
        })
                .withName("agent")
                .withTag("a", "b")
                .start();
        CounterSampler counterSampler = collector.getCounterSampler("counter");

        NumberSampler<Long> timePoint = collector.getNumberCollector("time", Long.class);
        AverageSampler averageSampler = collector.getAverageSampler("avg");
        SpeedSampler speedSampler = collector.getSpeedSampler("speed");

        for(int i = 0; i < 100; i++) {
            counterSampler.inc();
        }

        for(int i = 0; i < 100; i++) {
            averageSampler.add(i + 100L);
        }

        for(int i = 0; i < 100; i++) {
            speedSampler.add(i + 100L);
        }

        for(int i = 0; i < 100; i++) {
            timePoint.setValue(System.currentTimeMillis());
        }

//        System.out.println("speedPoint " + qpsTest(() -> {
//            speedPoint.add(3423);
//            speedPoint.value();
//        }));
//        System.out.println("averagePoint " + qpsTest(() -> {
//            averagePoint.add(3423);
//            averagePoint.value();
//        }));
//        System.out.println("timePoint " + qpsTest(() -> {
//            timePoint.setValue(System.currentTimeMillis());
//            timePoint.value();
//        }));
//        System.out.println("counter " + qpsTest(() -> {
//            counterPoint.inc();
//            counterPoint.value();
//        }));
    }
    public static long qpsTest(Runnable runnable) {
        long time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            runnable.run();
        }
        return System.currentTimeMillis() - time;
    }

    public int getPeriodSeconds() {
        return periodSeconds;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void stop() {
        idSamplerMap.clear();
        if(reportFuture != null) {
            reportFuture.cancel(true);
        }
    }

    public void pendingCollector() {
        this.status = CollectorStatus.PENDING;
    }

    public boolean isPending() {
        return this.status == CollectorStatus.PENDING;
    }



    static public enum CollectorStatus {
        PENDING
    }
}

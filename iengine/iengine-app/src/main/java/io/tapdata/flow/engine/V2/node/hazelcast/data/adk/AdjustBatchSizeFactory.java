package io.tapdata.flow.engine.V2.node.hazelcast.data.adk;

import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.tm.commons.dag.DAG;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.observable.logging.ObsLogger;
import org.apache.commons.lang3.StringUtils;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.openjdk.jol.info.GraphLayout;
import org.springframework.util.CollectionUtils;

import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/17 14:53 Create
 * @description
 */
public final class AdjustBatchSizeFactory {
    static final long DEFAULT_CHECK_INTERVAL_MS = 5000L;
    static final Map<String, ObsLogger> TASK_LOGGER_MAP = new ConcurrentHashMap<>(16);
    static final Map<String, AtomicBoolean> ADJUST_TASK_MAP = new ConcurrentHashMap<>(16);
    static final Map<String, AdjustManager> ADJUST_INSTANCE_MAP = new ConcurrentHashMap<>(16);
    static final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(20);
    static final Map<String, ScheduledFuture<?>> ADJUST_TASK_FUTURE_MAP = new ConcurrentHashMap<>(16);
    static final ExecutorService EXECUTOR_SERVICE = new ThreadPoolExecutor(
            50,
            200,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1000),
            r -> new Thread(r, AdjustBatchSizeFactory.class.getSimpleName()),
            new ThreadPoolExecutor.CallerRunsPolicy());

    private AdjustBatchSizeFactory() {

    }

    public static void register(String taskId, AdjustStage stage) {
        if (StringUtils.isBlank(taskId) || null == stage) {
            return;
        }
        ADJUST_INSTANCE_MAP.computeIfAbsent(
                taskId,
                key -> new AdjustManager(ADJUST_TASK_MAP.computeIfAbsent(taskId, k -> new AtomicBoolean(true)), DEFAULT_CHECK_INTERVAL_MS)
        ).append(taskId, stage);
    }

    public static void start(String taskId, ObsLogger obsLogger) {
        ADJUST_TASK_MAP.put(taskId, new AtomicBoolean(true));
        TASK_LOGGER_MAP.put(taskId, obsLogger);
        AdjustBatchSizeFactory.stopTaskIfNeed(taskId);
        final ScheduledFuture<?> scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(
                () -> {
                    try {
                        foreach(taskId, AdjustNodeInstance::checkOnce);
                    } catch (Exception e) {
                        Optional.ofNullable(TASK_LOGGER_MAP.get(taskId)).ifPresent(log -> log.warn("Check adjust batch size failed, error message: {}", e.getMessage()));
                    }
                },
                DEFAULT_CHECK_INTERVAL_MS,
                DEFAULT_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        ADJUST_TASK_FUTURE_MAP.put(taskId, scheduledFuture);
        obsLogger.info("Adjust batch size task started");
    }

    static void stopTaskIfNeed(String taskId) {
        Optional.ofNullable(ADJUST_TASK_FUTURE_MAP.get(taskId)).ifPresent(feature -> {
            try {
                feature.cancel(true);
            } catch (Exception e) {
                Optional.ofNullable(TASK_LOGGER_MAP.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
            }
        });
        try {
            ADJUST_TASK_FUTURE_MAP.remove(taskId);
        } catch (Exception e) {
            Optional.ofNullable(TASK_LOGGER_MAP.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
        }
    }

    public static void stop(String taskId) {
        try {
            Optional.ofNullable(ADJUST_TASK_MAP.get(taskId))
                    .ifPresent(e -> e.set(false));
        } catch (Exception e) {
            Optional.ofNullable(TASK_LOGGER_MAP.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
        }
        try {
            ADJUST_TASK_MAP.remove(taskId);
        } catch (Exception e) {
            Optional.ofNullable(TASK_LOGGER_MAP.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
        }
        AdjustBatchSizeFactory.stopTaskIfNeed(taskId);
        try {
            ADJUST_INSTANCE_MAP.remove(taskId);
        } catch (Exception e) {
            Optional.ofNullable(TASK_LOGGER_MAP.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
        }
        TASK_LOGGER_MAP.remove(taskId);
    }

    public static void unregister(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        try {
            ADJUST_INSTANCE_MAP.remove(taskId);
        } finally {
            AdjustBatchSizeFactory.stop(taskId);
        }
    }

    static void foreach(String taskId, Consumer<AdjustBatchSizeFactory.AdjustNodeInstance> consumer) {
        Optional.ofNullable(ADJUST_INSTANCE_MAP.get(taskId))
                .ifPresent(list -> list.foreach(consumer));
    }

    final static class AdjustManager {
        final AtomicBoolean isAlive;
        final long checkIntervalMs;

        private AdjustManager(AtomicBoolean isAlive, long checkIntervalMs) {
            this.isAlive = isAlive;
            this.checkIntervalMs = checkIntervalMs;
        }

        static final Map<String, AdjustNodeInstance> NODE_LIST = new ConcurrentHashMap<>(4);

        void foreach(Consumer<AdjustNodeInstance> consumer) {
            NODE_LIST.values().stream().filter(Objects::nonNull).forEach(consumer);
        }

        void append(String taskId, AdjustStage stage) {
            if (null == stage) {
                return;
            }
            NODE_LIST.put(stage.getNodeId(), new AdjustNodeInstance(isAlive, taskId, checkIntervalMs, stage));
        }

        boolean isEmpty() {
            return NODE_LIST.isEmpty();
        }
    }

    final static class AdjustNodeInstance {
        final LinkedBlockingQueue<AdjustInfo> queue;
        final FixedSizeQueue<Integer> historyQueue;
        final long checkIntervalMs;
        final AtomicBoolean isAlive;
        final AdjustStage stage;
        final String taskId;

        public AdjustNodeInstance(AtomicBoolean isAlive, String taskId, long checkIntervalMs, AdjustStage stage) {
            if (checkIntervalMs > 60000L) {
                checkIntervalMs = 60000L;
            }
            if (checkIntervalMs <= 0L) {
                checkIntervalMs = 5000L;
            }
            this.stage = stage;
            this.taskId = taskId;
            this.isAlive = isAlive;
            stage.setConsumer(this::accept);
            this.checkIntervalMs = checkIntervalMs;
            this.queue = new LinkedBlockingQueue<>(2000);
            this.historyQueue = new FixedSizeQueue<>(5, 5D);
        }

        void setAllTaskInfo(AdjustInfo info) {
            Optional.ofNullable(SpringUtil.getBean(TapdataTaskScheduler.class))
                    .map(TapdataTaskScheduler::getRunningTaskInfos)
                    .ifPresent(taskInfos ->
                            taskInfos.forEach(taskInfo -> {
                                info.allTaskCount++;
                                int nodeSize = Optional.ofNullable(taskInfo.getDag())
                                        .map(DAG::getNodes)
                                        .map(Collection::size)
                                        .orElse(0);
                                info.allTaskNodes += nodeSize;
                                if (Objects.equals(this.taskId, taskInfo.getId().toHexString())) {
                                    info.currentTaskNodes = nodeSize;
                                }
                            })
                    );
            Optional.ofNullable(SpringUtil.getBean(JvmMemoryService.class))
                    .ifPresent(jvmMemoryService -> {
                        final MemoryUsage heapUsage = jvmMemoryService.getHeapUsage();
                        info.sysMemUsed = heapUsage.getUsed();
                        info.sysMem = heapUsage.getMax();
                    });
        }

        public void accept(List<TapEvent> events) {
            final long delayAvg = delayAvg(events);
            try {
                CompletableFuture.runAsync(() -> {
                    final AdjustInfo adjust = adjust(events, delayAvg);
                    try {
                        queue.offer(adjust, 1000L, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Optional.ofNullable(TASK_LOGGER_MAP.get(taskId)).ifPresent(log -> log.warn("Offer adjust info failed, error message: {}", e.getMessage()));
                        Thread.currentThread().interrupt();
                    }
                }, EXECUTOR_SERVICE);
            } catch (Exception e) {
                Optional.ofNullable(TASK_LOGGER_MAP.get(taskId)).ifPresent(log -> log.warn("Offer adjust info failed, error message: {}", e.getMessage()));
            }
        }

        public long delayAvg(List<TapEvent> events) {
            if (CollectionUtils.isEmpty(events)) {
                return 0L;
            }
            final long now = System.currentTimeMillis();
            long total = 0L;
            for (TapEvent event : events) {
                if (event instanceof TapRecordEvent e) {
                    total += (now - e.getReferenceTime());
                } else {
                    total += (now - event.getTime());
                }
            }
            return total / events.size();
        }

        public AdjustInfo adjust(List<TapEvent> events, long delayAvg) {
            final AdjustInfo info = new AdjustInfo();
            info.eventSize = events.size();
            if (info.eventSize > 0) {
                info.eventMem = GraphLayout.parseInstance(events).totalSize();
                if (info.eventSize > 1) {
                    info.eventMemAvg = info.eventMem / info.eventSize;
                }
            }
            final AdjustStage.TaskInfo adjust = this.stage.getTaskInfo();
            info.batchSize = adjust.getIncreaseReadSize();
            info.eventQueueSize = adjust.getEventQueueSize();
            info.eventQueueCapacity = adjust.getEventQueueCapacity();
            info.eventDelay = delayAvg;
            info.eventQueueFullThreshold = adjust.getEventQueueFullThreshold();
            info.eventQueueIdleThreshold = adjust.getEventQueueIdleThreshold();
            info.eventDelayThresholdMs = adjust.getEventDelayThresholdMs();
            info.taskMemThreshold = adjust.getTaskMemThreshold();
            return info;
        }

        void checkOnce() {
            if (queue.isEmpty() || isAlive == null) {
                return;
            }
            long now = System.currentTimeMillis();
            final List<AdjustInfo> adjustInfos = new ArrayList<>();
            while (isAlive.get()) {
                AdjustInfo adjustInfo = queue.peek();
                if (null == adjustInfo) {
                    break;
                }
                if (now >= adjustInfo.timestamp) {
                    AdjustInfo item = queue.poll();
                    adjustInfos.add(item);
                } else {
                    break;
                }
            }
            if (!adjustInfos.isEmpty()) {
                final AdjustStage.MetricInfo metricInfo = new AdjustStage.MetricInfo();
                final AdjustInfo avg = AdjustInfo.avg(adjustInfos);
                final JudgeResult result = AdjustBatchSizeFactory.judge(avg, taskId);
                if (result.type == 0) {
                    return;
                }
                int ago = avg.batchSize;
                int size = ((Number) (ago * (1.0d + result.rate))).intValue();
                if (size <= 5 && size == ago) {
                    size += (result.type == -2 ? -1 : 1);
                }
                setAllTaskInfo(avg);
                //检查内存是否满足新的batchSize
                long mem = avg.sysMem - avg.sysMemUsed;
                long available = ((Number) ((0.1d * mem / avg.allTaskNodes * avg.currentTaskNodes) * 0.8d)).longValue();
                long needMem = avg.eventMemAvg * ((size * 2L) >> 1);
                if (needMem > available) {
                    size = ((Number) (available / avg.eventMemAvg)).intValue();
                }
                size = fixNumber(size);
                size = historyQueue.push(size);
                metricInfo.setIncreaseReadSize(size);
                this.stage.updateIncreaseReadSize(metricInfo.getIncreaseReadSize());
                this.stage.metric(metricInfo);
            }
        }
    }

    static int fixNumber(int num) {
        if (num <= 10) return num;
        int item = (int) Math.pow(10, (int) Math.log10(num));
        int step = item / 2;
        int offset = num % item;
        return num - offset + (offset > step ? step : 0);
    }

    static JudgeResult judge(AdjustInfo adjustInfo, String taskId) {
        long now = System.currentTimeMillis();
        JudgeResult result = new JudgeResult();
        double rateOf = 0d;
        //1) If the batch returned is smaller than the batch size,
        // that means some kind of time-out occurred and we can consider to lower the batch size
        if (adjustInfo.batchSize > adjustInfo.eventSize) {
            result.type = -2;
            rateOf = -1.0d * (adjustInfo.batchSize - adjustInfo.eventSize) / adjustInfo.batchSize;
            double r = rateOf;
            Optional.ofNullable(TASK_LOGGER_MAP.get(taskId))
                    .ifPresent(log -> log.info("Judgment Rule 1 - [{} - {}], rate of {} , judge info: {}", now, taskId, r, JSON.toJSONString(adjustInfo)));
        }
        //2) If any of the hazelcast queues fill up to over 95% of their limit,
        // then we can consider to lower the batch size
        double rate = 1.0D * adjustInfo.eventQueueSize / adjustInfo.eventQueueCapacity;
        if (adjustInfo.batchSize > 1 && rate > adjustInfo.eventQueueFullThreshold) {
            //Isn't the delay exceeding the threshold here, and the capacity has not reached 70%, such as 20%,
            // with a default queue limit of 95%?
            // At this point, available=95% -20%=75%, indicating that it can only increase by another 75% at most.
            rateOf = rateOf + rate / adjustInfo.eventQueueFullThreshold;
            result.type = -1;
            double r = rateOf;
            Optional.ofNullable(TASK_LOGGER_MAP.get(taskId))
                    .ifPresent(log -> log.info("Judgment Rule 2 - [{} - {}], rate of {} , judge info: {}", now, taskId, r, JSON.toJSONString(adjustInfo)));
        }
        //3) If the data latency is higher than a threshold (e.g., 1 second),
        // and none of the hazelcast queues is filled above 70%,
        // then consider to increase the batch size
        if (adjustInfo.eventDelay > adjustInfo.eventDelayThresholdMs && rate < adjustInfo.eventQueueIdleThreshold) {
            double available = (adjustInfo.eventQueueFullThreshold - rate);
            if (Math.abs(available) < Math.abs(rateOf)) {
                result.rate = rateOf;
                double r = rateOf;
                double a = available;
                Optional.ofNullable(TASK_LOGGER_MAP.get(taskId))
                        .ifPresent(log -> log.info("Judgment Rule 3 - [{} - {}] available less than rateOf, rate of {}, available: {}, judge info: {}", now, taskId, r, a, JSON.toJSONString(adjustInfo)));
                return result;
            }
            if (result.type >= 0) {
                available = available / 2;
            }
            //According to the delay ratio, if the delay is too high,
            // it is not advisable to adjust the queue too many times,
            // otherwise the queue will always be full and will accumulate batches after timeout. The delay will stabilize at around 1 second
            double downRate = -1.0D * (adjustInfo.eventDelay - adjustInfo.eventDelayThresholdMs) /adjustInfo.eventDelay;
            //rateOf + avaliable, The value of rateOf here mainly comes from the first rule,
            // which is that the number of batches is greater than the number of events,
            // and then a ratio can be obtained,
            // which needs to be adjusted to make the number of batches as equal to the number of events as possible,
            // so as to achieve relative balance.
            rateOf = rateOf + available + downRate;
            result.type = 1;
            double r = rateOf;
            double a = available;
            Optional.ofNullable(TASK_LOGGER_MAP.get(taskId))
                    .ifPresent(log -> log.info("Judgment Rule 3 - [{} - {}], rate of {}, available: {}, down rate: {}, judge info: {}", now, taskId, r, a, downRate, JSON.toJSONString(adjustInfo)));
        }
        result.rate = rateOf;
        return result;
    }

    static class JudgeResult {
        int type;
        double rate;
    }

    static class AdjustInfo {
        final long timestamp;
        //number of events
        int eventSize;
        //Current batch count
        int batchSize;

        //The size of the event queue has been used
        int eventQueueSize;
        //Maximum capacity of event queue
        int eventQueueCapacity;
        //Determine the threshold for event queue to be full, should more than 'eventQueueIdleThreshold' and less than 1.0
        double eventQueueFullThreshold = 0.95D;
        //Determine the threshold for idle event queue, should more than 0.0, and less than 'eventQueueFullThreshold'
        double eventQueueIdleThreshold = 0.7D;

        //Average delay of events
        long eventDelay;
        //Threshold for determining excessive event delay, should be more than 0
        double eventDelayThresholdMs = 800L;

        //Current number of task nodes
        int currentTaskNodes;
        //The sum of all task nodes on the current engine
        int allTaskNodes;
        //The current number of tasks on the engine
        int allTaskCount;

        //Total memory usage of the event
        long eventMem;
        //Average memory usage of events
        long eventMemAvg;
        //Current total memory usage of the engine
        long sysMem;
        //The current engine is using memory
        long sysMemUsed;
        //Determine the threshold for high task memory, should more than 0.0, and less than 1.0
        double taskMemThreshold = 0.8D;

        public AdjustInfo() {
            this.timestamp = System.currentTimeMillis();
        }

        public static AdjustInfo avg(List<AdjustInfo> adjustInfos) {
            if (CollectionUtils.isEmpty(adjustInfos)) {
                return new AdjustInfo();
            }
            if (adjustInfos.size() == 1) {
                return adjustInfos.get(0);
            }
            AdjustInfo adjustInfo = new AdjustInfo();
            for (AdjustInfo item : adjustInfos) {
                adjustInfo.eventSize += item.eventSize;
                adjustInfo.batchSize = item.batchSize;
                adjustInfo.eventQueueSize = item.eventQueueSize;
                adjustInfo.eventQueueCapacity = item.eventQueueCapacity;
                adjustInfo.eventQueueFullThreshold = item.eventQueueFullThreshold;
                adjustInfo.eventQueueIdleThreshold = item.eventQueueIdleThreshold;
                adjustInfo.eventDelay = Math.max(item.eventDelay, adjustInfo.eventDelay);
                adjustInfo.eventDelayThresholdMs = item.eventDelayThresholdMs;
                adjustInfo.eventMem += item.eventMem;
                adjustInfo.eventMemAvg += item.eventMemAvg;
                adjustInfo.taskMemThreshold = item.taskMemThreshold;
            }
            int size = adjustInfos.size();
            adjustInfo.eventSize = adjustInfo.eventSize / size;
            adjustInfo.eventMem = adjustInfo.eventMem / size;
            adjustInfo.eventMemAvg = adjustInfo.eventMemAvg / size;
            return adjustInfo;
        }
    }
}

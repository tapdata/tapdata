package io.tapdata.flow.engine.V2.node.hazelcast.data.adk;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.dag.DAG;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.observable.logging.ObsLogger;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jol.info.GraphLayout;
import org.springframework.util.CollectionUtils;

import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
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
    static final Map<String, AtomicBoolean> ADJUST_TASK_MAP = new HashMap<>(16);
    static final Map<String, AdjustManager> ADJUST_INSTANCE_MAP = new HashMap<>(16);
    static final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    static final Map<String, ScheduledFuture<?>> ADJUST_TASK_FUTURE_MAP = new HashMap<>(16);
    static final ExecutorService EXECUTOR_SERVICE = new ThreadPoolExecutor(
            50,
            200,
            0L,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(true),
            r -> new Thread(r, AdjustBatchSizeFactory.class.getSimpleName()));

    private AdjustBatchSizeFactory() {

    }

    public static void register(String taskId, AdjustStage stage) {
        if (StringUtils.isBlank(taskId) || null == stage) {
            return;
        }
        ADJUST_INSTANCE_MAP.computeIfAbsent(
                taskId,
                key -> new AdjustManager(ADJUST_TASK_MAP.get(taskId), DEFAULT_CHECK_INTERVAL_MS)
        ).append(taskId, stage);
        ADJUST_TASK_MAP.putIfAbsent(taskId, new AtomicBoolean(true));
    }

    public static void startIfNeed(String taskId, ObsLogger obsLogger) {
        boolean needStart = Optional.ofNullable(ADJUST_INSTANCE_MAP.get(taskId))
                .map(AdjustManager::isEmpty)
                .orElse(true);
        if (!needStart) {
            return;
        }
        AdjustBatchSizeFactory.stopTaskIfNeed(taskId);
        final ScheduledFuture<?> scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(
                () -> foreach(taskId, AdjustNodeInstance::checkOnce),
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
                //@todo log info
            }
        });
        try {
            ADJUST_TASK_FUTURE_MAP.remove(taskId);
        } catch (Exception e) {
            //@todo log info
        }
    }

    public static void stop(String taskId) {
        try {
            Optional.ofNullable(ADJUST_TASK_MAP.get(taskId))
                    .ifPresent(e -> e.set(false));
        } catch (Exception e) {
            //@todo log info
        }
        try {
            ADJUST_TASK_MAP.remove(taskId);
        } catch (Exception e) {
            //@todo log info
        }
        AdjustBatchSizeFactory.stopTaskIfNeed(taskId);
        try {
            ADJUST_INSTANCE_MAP.remove(taskId);
        } catch (Exception e) {
            //@todo log info
        }
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

        static final Map<String, AdjustNodeInstance> NODE_LIST = new HashMap<>(4);

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
        }

        public void accept(List<TapEvent> events) {
            final long delayAvg = delayAvg(events);
            CompletableFuture.runAsync(() -> {
                final AdjustInfo adjust = adjust(events, delayAvg);
                try {
                    queue.offer(adjust, 1000L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    //@todo log info
                    Thread.currentThread().interrupt();
                }
            }, EXECUTOR_SERVICE);
        }

        public long delayAvg(List<TapEvent> events) {
            if (CollectionUtils.isEmpty(events)) {
                return 0L;
            }
            final long now = System.currentTimeMillis();
            long total = 0L;
            for (TapEvent event : events) {
                total += (now - event.getTime());
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
            return info;
        }

        void checkOnce() {
            if (queue.isEmpty()) {
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
                final int scope = AdjustBatchSizeFactory.judge(avg);
                int size;
                if (scope > 8) {
                    size = ((Number) (avg.batchSize * 0.9D)).intValue();
                } else if (scope > 5) {
                    size = ((Number) (avg.batchSize * 0.95D)).intValue();
                } else if (scope > 2) {
                    size = ((Number) (avg.batchSize * 1.15D)).intValue();
                } else {
                    return;
                }
                setAllTaskInfo(avg);
                //检查内存是否满足新的batchSize
                long mem = avg.sysMem - avg.sysMemUsed;
                long available = ((Number) ((0.1d * mem / avg.allTaskNodes * avg.currentTaskNodes) * 0.8d)).longValue();
                long needMem = avg.eventMemAvg * ((size * 2L) >> 1);
                if (needMem > available) {
                    size = ((Number) (available / avg.eventMemAvg)).intValue();
                }
                metricInfo.setIncreaseReadSize(size);
                this.stage.updateIncreaseReadSize(metricInfo.getIncreaseReadSize());
                this.stage.metric(metricInfo);
            }
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
    }

    static int judge(AdjustInfo adjustInfo) {
        int scope = 0;
        if (adjustInfo.batchSize > adjustInfo.eventSize) {
            scope += 8;
        }
        double rate = 1.0D * adjustInfo.eventQueueSize / adjustInfo.eventQueueCapacity;
        if (rate > adjustInfo.eventQueueSizeThreshold) {
            scope += 5;
        }
        if (adjustInfo.eventDelay > adjustInfo.eventDelayThreshold && rate < 0.7D) {
            scope += -5;
        }
        return scope;
    }

    final static class AdjustInfo {
        final long timestamp;
        int eventSize;
        int batchSize;

        int eventQueueSize;
        int eventQueueCapacity;
        double eventQueueSizeThreshold;

        long eventDelay;
        double eventDelayThreshold;

        int currentTaskNodes;
        int allTaskNodes;
        int allTaskCount;

        long eventMem;
        long eventMemAvg;
        long sysMem;
        long sysMemUsed;
        double taskMemThreshold;


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
                adjustInfo.eventQueueSizeThreshold = item.eventQueueSizeThreshold;
                adjustInfo.eventDelay += item.eventDelay;
                adjustInfo.eventDelayThreshold = item.eventDelayThreshold;
                adjustInfo.eventMem += item.eventMem;
                adjustInfo.eventMemAvg += item.eventMemAvg;
                adjustInfo.taskMemThreshold = item.taskMemThreshold;
            }
            int size = adjustInfos.size();
            adjustInfo.eventSize = adjustInfo.eventSize / size;
            adjustInfo.eventDelay = adjustInfo.eventDelay / size;
            adjustInfo.eventMem = adjustInfo.eventMem / size;
            adjustInfo.eventMemAvg = adjustInfo.eventMemAvg / size;
            return adjustInfo;
        }
    }
}

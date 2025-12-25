package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.dag.DAG;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.AdjustBatchSizeFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.AdjustStage;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.FixedSizeQueue;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.JvmMemoryService;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.JudgeResult;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.observable.metric.handler.HandlerUtil;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import org.openjdk.jol.info.GraphLayout;
import org.springframework.util.CollectionUtils;

import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/30 09:50 Create
 * @description
 */
public final class IncreaseRuleInstance {
    static final long CHECK_INTERVAL_MS_MAX = 60000L;
    static final long CHECK_INTERVAL_MS_MIN = 5000L;
    static final int QUEUE_SIZE = 2000;
    static final int HISTORY_QUEUE_SIZE = 5;
    static final double STABLE_THRESHOLD = 5D;

    final LinkedBlockingQueue<AdjustInfo> queue;
    final FixedSizeQueue<Integer> historyQueue;
    final long checkIntervalMs;
    final AtomicBoolean isAlive;
    final AdjustStage stage;
    final String taskId;
    long lastChangeTime = 0L;
    ThreadPoolExecutorEx sourceRunner;

    public IncreaseRuleInstance(AtomicBoolean isAlive, String taskId, long checkIntervalMs, AdjustStage stage) {
        if (checkIntervalMs > CHECK_INTERVAL_MS_MAX) {
            checkIntervalMs = CHECK_INTERVAL_MS_MAX;
        }
        if (checkIntervalMs <= 0L) {
            checkIntervalMs = CHECK_INTERVAL_MS_MIN;
        }
        this.stage = stage;
        this.taskId = taskId;
        this.isAlive = isAlive;
        stage.setConsumer(this::accept);
        this.checkIntervalMs = checkIntervalMs;
        this.queue = new LinkedBlockingQueue<>(QUEUE_SIZE);
        this.historyQueue = new FixedSizeQueue<>(HISTORY_QUEUE_SIZE, STABLE_THRESHOLD);
    }

    public IncreaseRuleInstance sourceRunner(ThreadPoolExecutorEx sourceRunner) {
        this.sourceRunner = sourceRunner;
        return this;
    }

    void setAllTaskInfo(AdjustInfo info) {
        Optional.ofNullable(SpringUtil.getBean(TapdataTaskScheduler.class))
                .map(TapdataTaskScheduler::getRunningTaskInfos)
                .ifPresent(taskInfos ->
                        taskInfos.forEach(taskInfo -> {
                            info.setAllTaskCount(info.getAllTaskCount() + 1);
                            int nodeSize = Optional.ofNullable(taskInfo.getDag())
                                    .map(DAG::getNodes)
                                    .map(Collection::size)
                                    .orElse(0);
                            info.setAllTaskNodes(info.getAllTaskNodes() + nodeSize);
                            if (Objects.equals(this.taskId, taskInfo.getId().toHexString())) {
                                info.setCurrentTaskNodes(info.getCurrentTaskNodes() + nodeSize);
                            }
                        })
                );
        Optional.ofNullable(SpringUtil.getBean(JvmMemoryService.class))
                .ifPresent(jvmMemoryService -> {
                    final MemoryUsage heapUsage = jvmMemoryService.getHeapUsage();
                    info.setSysMem(heapUsage.getMax());
                    info.setSysMemUsed(heapUsage.getUsed());
                });
    }

    public void accept(List<TapEvent> events) {
        if (queue.size() >= 100) {
            return;
        }
        try {
            //this.sourceRunner.submit(() -> {
                final long delayAvg = delayAvg(events);
                final AdjustInfo adjust = adjust(events, delayAvg);
                try {
                    if (queue.offer(adjust)) {
                        //do nothing
                    }
                } catch (Exception e) {
                    AdjustBatchSizeFactory.warn(taskId, "Offer adjust info failed, error message: {}", e.getMessage());
                }
            //});
        } catch (Exception e) {
            AdjustBatchSizeFactory.warn(taskId, "Offer adjust info failed, error message: {}", e.getMessage());
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
        final AdjustInfo info = new AdjustInfo(taskId);
        info.setEventSize(events.size());
        if (info.getEventSize() > 0) {
            TapEvent tapEvent = events.get(0);
            if (tapEvent instanceof TapRecordEvent e) {
                info.setEventMemAvg(e.getMemorySize());
            } else {
                info.setEventMemAvg(GraphLayout.parseInstance(tapEvent).totalSize());
            }
            info.setEventMem(info.getEventMemAvg() * info.getEventSize());
        }
        final AdjustStage.TaskInfo adjust = this.stage.getTaskInfo();
        info.setBatchSize(adjust.getIncreaseReadSize());
        info.setEventQueueSize(adjust.getEventQueueSize());
        info.setEventQueueCapacity(adjust.getEventQueueCapacity());
        info.setEventDelay(delayAvg);
        info.setEventQueueFullThreshold(adjust.getEventQueueFullThreshold());
        info.setEventQueueIdleThreshold(adjust.getEventQueueIdleThreshold());
        info.setEventDelayThresholdMs(adjust.getEventDelayThresholdMs());
        info.setTaskMemThreshold(adjust.getTaskMemThreshold());
        return info;
    }

    boolean exit() {
        return queue.isEmpty() || isAlive == null;
    }

    void collect(List<AdjustInfo> adjustInfos, long now) {
        AdjustInfo adjustInfo = null;
        do {
            adjustInfo = queue.poll();
            if (null != adjustInfo) {
                adjustInfos.add(adjustInfo);
                if (now < adjustInfo.getTimestamp()) {
                    break;
                }
            }
        } while (isAlive.get() && null != adjustInfo);
    }

    public void checkOnce() {
        if (exit()) {
            return;
        }
        long now = System.currentTimeMillis();
        final List<AdjustInfo> adjustInfos = new ArrayList<>();
        collect(adjustInfos, now);
        if (!adjustInfos.isEmpty()) {
            final AdjustStage.MetricInfo metricInfo = new AdjustStage.MetricInfo();
            final AdjustInfo avg = AdjustInfo.avg(taskId, adjustInfos);
            IncreaseRuleFactory.factory().loadOneByOne(adjustInfos, avg);
            JudgeResult result = new JudgeResult();
            IncreaseRuleFactory.factory().each(avg, result);
            if (!result.isHasJudge()) {
                return;
            }
            int ago = avg.getBatchSize();
            int calcItem = ago;
            if (calcItem < 10 && result.getRate() > 0D) {
                calcItem = 10;
            }
            int size = ((Number) (calcItem * (1.0d + result.getRate()))).intValue();
            if (size <= 5 && size == calcItem) {
                size += (result.getType() == -2 ? -1 : 1);
            }
            setAllTaskInfo(avg);
            //Check if the memory meets the new batchSize requirements
            long mem = avg.getSysMem() - avg.getSysMemUsed();
            long available = ((Number) ((0.1d * mem / avg.getAllTaskNodes() * avg.getCurrentTaskNodes()) * 0.8d)).longValue();
            long needMem = avg.getEventMemAvg() * ((size * 2L) >> 1);
            if (needMem > available) {
                size = ((Number) (available / avg.getEventMemAvg())).intValue();
            }
            size = fixNumber(size);
            size = historyQueue.push(size);
            metricInfo.setIncreaseReadSize(size);
            int finalSize = size;
            logChangeInfoIfNeed(avg, size, () -> {
                String msg = "The increase read size has changed from {} to {}, reason: {}";
                AdjustBatchSizeFactory.info(taskId, msg, ago, finalSize, result.getReason());
            });
            this.stage.updateIncreaseReadSize(metricInfo.getIncreaseReadSize());
            this.stage.metric(metricInfo);
        }
    }

    void logChangeInfoIfNeed(AdjustInfo adjustInfo, int newSize, Runnable runnable) {
        int left = adjustInfo.getBatchSize();
        int right = newSize;
        if (right == left) {
            return;
        }
        if (right < left) {
            left = right;
            right = adjustInfo.getBatchSize();
        }
        long now = System.currentTimeMillis();
        double rate = 1.0D * Math.abs(right - left) / left;
        long times = now - this.lastChangeTime;
        if (this.lastChangeTime <= 0L || (rate >= .5d && times >= CHECK_INTERVAL_MS_MAX) || rate >= 1d) {
            runnable.run();
            this.lastChangeTime = now;
        }
    }

    static int fixNumber(int num) {
        if (num <= 0) return 1;
        if (num <= 10) return num;
        int item = (int) Math.pow(10, (int) Math.log10(num));
        int fix = num % item;
        if (fix == 0) {
            return num;
        }
        int base = num / item * item;
        if (num < 50) {
            return base + (fix >= 5 ? 10 : 5);
        }
        if (num < 100) {
            return base + (fix >= 5 ? 5 : 0);
        }
        if (fix > (3 * item / 4)) {
            return base + item;
        }
        if (fix > item / 2) {
            return base + 3 * item / 4;
        }
        if (fix > item / 4) {
            return base + item / 2;
        }
        if (fix < item / 10) {
            return base;
        }
        return base + item / 4;
    }
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.adk;

import io.tapdata.flow.engine.V2.node.hazelcast.data.adk.rule.IncreaseRuleInstance;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import org.apache.commons.lang3.StringUtils;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
    private final Map<String, ObsLogger> taskLoggerMap = new ConcurrentHashMap<>(16);
    private final Map<String, AtomicBoolean> adjustTaskMap = new ConcurrentHashMap<>(16);
    private final Map<String, AdjustManager> adjustInstanceMap = new ConcurrentHashMap<>(16);
    private final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(20);
    private final Map<String, ScheduledFuture<?>> adjustTaskFutureMap = new ConcurrentHashMap<>(16);

    private AdjustBatchSizeFactory() {

    }

    private static volatile AdjustBatchSizeFactory INSTANCE;

    public static AdjustBatchSizeFactory getInstance() {
        if (INSTANCE == null) {
            synchronized (AdjustBatchSizeFactory.class) {
                if (INSTANCE == null) {
                    INSTANCE = new AdjustBatchSizeFactory();
                }
            }
        }
        return INSTANCE;
    }

    public static void register(String taskId, AdjustStage stage, ThreadPoolExecutorEx sourceRunner) {
        if (StringUtils.isBlank(taskId) || null == stage) {
            return;
        }
        final AdjustBatchSizeFactory instance = getInstance();
        instance.adjustInstanceMap.computeIfAbsent(
                taskId,
                key -> new AdjustManager(instance.adjustTaskMap.computeIfAbsent(taskId, k -> new AtomicBoolean(true)), DEFAULT_CHECK_INTERVAL_MS)
        ).append(taskId, stage, sourceRunner);
    }

    public static void start(String taskId, ObsLogger obsLogger) {
        getInstance().adjustTaskMap.put(taskId, new AtomicBoolean(true));
        getInstance().taskLoggerMap.put(taskId, obsLogger);
        AdjustBatchSizeFactory.stopTaskIfNeed(taskId);
        final ScheduledFuture<?> scheduledFuture = getInstance().scheduledExecutor.scheduleWithFixedDelay(
                () -> {
                    try {
                        foreach(taskId, IncreaseRuleInstance::checkOnce);
                    } catch (Exception e) {
                        Optional.ofNullable(getInstance().taskLoggerMap.get(taskId)).ifPresent(log -> log.debug("Check adjust batch size failed, error message: {}", e.getMessage()));
                    }
                },
                DEFAULT_CHECK_INTERVAL_MS,
                DEFAULT_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        getInstance().adjustTaskFutureMap.put(taskId, scheduledFuture);
        obsLogger.info("Adjust batch size task started");
    }

    static void stopTaskIfNeed(String taskId) {
        final AdjustBatchSizeFactory instance = getInstance();
        Optional.ofNullable(instance.adjustTaskFutureMap.get(taskId)).ifPresent(feature -> {
            try {
                feature.cancel(true);
            } catch (Exception e) {
                Optional.ofNullable(instance.taskLoggerMap.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
            }
        });
        try {
            instance.adjustTaskFutureMap.remove(taskId);
        } catch (Exception e) {
            Optional.ofNullable(instance.taskLoggerMap.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
        }
    }

    public static void stop(String taskId) {
        final AdjustBatchSizeFactory instance = getInstance();
        try {
            Optional.ofNullable(instance.adjustTaskMap.get(taskId))
                    .ifPresent(e -> e.set(false));
        } catch (Exception e) {
            Optional.ofNullable(instance.taskLoggerMap.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
        }
        try {
            instance.adjustTaskMap.remove(taskId);
        } catch (Exception e) {
            Optional.ofNullable(instance.taskLoggerMap.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
        }
        AdjustBatchSizeFactory.stopTaskIfNeed(taskId);
        try {
            instance.adjustInstanceMap.remove(taskId);
        } catch (Exception e) {
            Optional.ofNullable(instance.taskLoggerMap.get(taskId)).ifPresent(log -> log.warn("Stop adjust batch size task failed, error message: {}", e.getMessage()));
        }
        instance.taskLoggerMap.remove(taskId);
    }

    public static void unregister(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        final AdjustBatchSizeFactory instance = getInstance();
        try {
            instance.adjustInstanceMap.remove(taskId);
        } finally {
            AdjustBatchSizeFactory.stop(taskId);
        }
    }

    static void foreach(String taskId, Consumer<IncreaseRuleInstance> consumer) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        final AdjustBatchSizeFactory instance = getInstance();
        Optional.ofNullable(instance.adjustInstanceMap.get(taskId))
                .ifPresent(list -> list.foreach(consumer));
    }

    final static class AdjustManager {
        final AtomicBoolean isAlive;
        final long checkIntervalMs;

        private AdjustManager(AtomicBoolean isAlive, long checkIntervalMs) {
            this.isAlive = isAlive;
            this.checkIntervalMs = checkIntervalMs;
        }

        static final Map<String, IncreaseRuleInstance> NODE_LIST = new ConcurrentHashMap<>(4);

        void foreach(Consumer<IncreaseRuleInstance> consumer) {
            NODE_LIST.values().stream().filter(Objects::nonNull).forEach(consumer);
        }

        void append(String taskId, AdjustStage stage, ThreadPoolExecutorEx sourceRunner) {
            if (null == stage) {
                return;
            }
            NODE_LIST.put(stage.getNodeId(), new IncreaseRuleInstance(isAlive, taskId, checkIntervalMs, stage).sourceRunner(sourceRunner));
        }
    }

    public static void debug(String taskId, String msg, Object... params) {
        Optional.ofNullable(getInstance().taskLoggerMap.get(taskId)).ifPresent(log -> log.debug(msg, params));
    }

    public static void info(String taskId, String msg, Object... params) {
        Optional.ofNullable(getInstance().taskLoggerMap.get(taskId)).ifPresent(log -> log.info(msg, params));
    }

    public static void warn(String taskId, String msg, Object... params) {
        Optional.ofNullable(getInstance().taskLoggerMap.get(taskId)).ifPresent(log -> log.warn(msg, params));
    }
}

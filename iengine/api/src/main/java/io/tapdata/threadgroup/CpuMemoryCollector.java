package io.tapdata.threadgroup;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.Usage;
import io.tapdata.pdk.core.executor.ThreadFactory;
import io.tapdata.threadgroup.utils.ThreadGroupUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.openjdk.jol.info.GraphLayout;
import org.springframework.util.CollectionUtils;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/25 10:02 Create
 * @description
 */
@Slf4j
public final class CpuMemoryCollector {
    private static final double JUDGE_CHANGE_RATE = 0.25d;
    private static final ExecutorService EXECUTOR_SERVICE = new ThreadPoolExecutor(
            50,
            200,
            0L,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            r -> new Thread(r, "CpuMemoryCollector"));
    public static final ThreadCPUMonitor THREAD_CPU_TIME = new ThreadCPUMonitor();
    public static final CpuMemoryCollector COLLECTOR = new CpuMemoryCollector();
    final Map<String, String> taskWithNode = new HashMap<>(16);
    final Map<String, WeakReference<TaskDto>> taskDtoMap = new HashMap<>(16);
    final Map<String, List<WeakReference<Object>>> weakReferenceMap = new HashMap<>(16);
    final Map<String, List<WeakReference<ThreadFactory>>> threadGroupMap = new HashMap<>(16);
    final Map<String, Info> taskInfo = new HashMap<>(16);
    final Map<WeakReference<Object>, MemInfo> cacheMemoryMap = new HashMap<>(16);

    static class MemInfo {
        long mem = 0L;
        Long lastCalcTime;
        final WeakReference<Object> weakReference;

        public MemInfo(WeakReference<Object> weakReference) {
            this.weakReference = weakReference;
        }

        public Long memory() {
            if (weakReference == null) {
                return null;
            }
            final Object object = weakReference.get();
            if (object == null) {
                synchronized (weakReference) {
                    COLLECTOR.cacheMemoryMap.remove(weakReference);
                }
                return null;
            }
            //30s内使用旧值，避免频繁计算
            if (null != lastCalcTime && 30000L >= (System.currentTimeMillis() - lastCalcTime)) {
                return mem;
            }
            if (null == lastCalcTime) {
                lastCalcTime = System.currentTimeMillis();
            }
            long sizeOf;
            try {
                sizeOf = GraphLayout.parseInstance(object).totalSize();
            } catch (Exception e) {
                sizeOf = RamUsageEstimator.sizeOfObject(object);
            }
            return mem = sizeOf;
        }
    }

    static class Info {
        String taskId;
        Long lastCount = 0L;

        public boolean judged(long size) {
            if (lastCount <= 0L) {
                lastCount = size;
                return true;
            }
            final double rate = Math.abs(size - lastCount) * 1.0D / lastCount;
            if (rate >= JUDGE_CHANGE_RATE) {
                lastCount = size;
                return true;
            }
            return false;
        }
    }

    private CpuMemoryCollector() {

    }


    public static void startTask(TaskDto taskDto) {
        final Info item = new Info();
        item.taskId = taskDto.getId().toHexString();
        COLLECTOR.taskDtoMap.put(item.taskId, new WeakReference<>(taskDto));
        COLLECTOR.taskInfo.put(item.taskId, item);
    }

    public static void addNode(String taskId, String nodeId) {
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(taskId)) {
            return;
        }
        COLLECTOR.taskWithNode.put(nodeId, taskId);
    }

    public static void registerTask(String nodeId, ThreadFactory threadGroup) {
        if (null == threadGroup) {
            return;
        }
        final String taskId = COLLECTOR.taskWithNode.get(nodeId);
        if (StringUtils.isEmpty(taskId)) {
            return;
        }
        final List<WeakReference<ThreadFactory>> weakReferences = COLLECTOR.threadGroupMap.computeIfAbsent(taskId, k -> new ArrayList<>());
        synchronized (weakReferences) {
            weakReferences.removeIf(weakReference -> null == weakReference.get());
        }
        for (WeakReference<ThreadFactory> weakReference : weakReferences) {
            if (weakReference.get() == threadGroup) {
                return;
            }
        }
        synchronized (weakReferences) {
            weakReferences.add(new WeakReference<>(threadGroup));
        }
    }


    public static void unregisterTask(String taskId) {
        COLLECTOR.threadGroupMap.remove(taskId);
        final List<WeakReference<Object>> weakReferences = COLLECTOR.weakReferenceMap.get(taskId);
        if (!CollectionUtils.isEmpty(weakReferences)) {
            weakReferences.forEach(COLLECTOR.cacheMemoryMap::remove);
        }
        COLLECTOR.weakReferenceMap.remove(taskId);
        COLLECTOR.taskDtoMap.remove(taskId);
        COLLECTOR.taskInfo.remove(taskId);
        final List<String> nodeIds = new ArrayList<>(COLLECTOR.taskWithNode.keySet());
        nodeIds.forEach(nodeId -> {
            if (Objects.equals(taskId, COLLECTOR.taskWithNode.get(nodeId))) {
                COLLECTOR.taskWithNode.remove(nodeId);
            }
        });
    }

    public static void listening(String nodeId, Object info) {
        final String taskId = COLLECTOR.taskWithNode.get(nodeId);
        if (StringUtils.isEmpty(taskId)) {
            return;
        }
        final List<WeakReference<Object>> weakReferences = COLLECTOR.weakReferenceMap.computeIfAbsent(taskId, k -> new LinkedList<>());
        final WeakReference<Object> reference = new WeakReference<>(info);
        weakReferences.add(reference);
        final MemInfo memInfo = new MemInfo(reference);
        COLLECTOR.cacheMemoryMap.put(reference, memInfo);
    }

    void eachTaskOnce(List<WeakReference<Object>> weakReferences, List<WeakReference<Object>> remove, Usage usage) {
        weakReferences.stream()
                .filter(Objects::nonNull)
                .forEach(weakReference -> {
                    if (null == weakReference.get()) {
                        remove.add(weakReference);
                        return;
                    }
                    ignore(() -> {
                        final MemInfo memInfo = COLLECTOR.cacheMemoryMap.get(weakReference);
                        if (null != memInfo) {
                            final long sizeOf = memInfo.memory();
                            final long value = usage.getHeapMemoryUsage() + sizeOf;
                            usage.setHeapMemoryUsage(value);
                        } else {
                            remove.add(weakReference);
                        }
                    }, "Calculate memory usage failed, {}");
                });
    }

    void collectMemoryUsage(List<String> filterTaskIds, Map<String, Usage> usageMap) {
        final List<String> taskIds = new ArrayList<>(weakReferenceMap.keySet());
        taskIds.stream()
                .filter(id -> CollectionUtils.isEmpty(filterTaskIds) || filterTaskIds.contains(id))
                .filter(taskId -> {
                    Info item = COLLECTOR.taskInfo.get(taskId);
                    if (null == item) {
                        item = new Info();
                        item.taskId = taskId;
                        COLLECTOR.taskInfo.put(taskId, item);
                    }
                    List<WeakReference<Object>> weakReferences = COLLECTOR.weakReferenceMap.get(taskId);
                    if (CollectionUtils.isEmpty(weakReferences)) {
                        return false;
                    }
                    synchronized (weakReferences) {
                        weakReferences.removeIf(weakReference -> null == weakReference.get());
                    }
                    return item.judged(weakReferences.size());
                }).forEach(taskId -> {
                    //CompletableFuture<Void> futureItem = CompletableFuture.runAsync(() -> {
                    final Usage usage = usageMap.computeIfAbsent(taskId, k -> new Usage());
                    Optional.ofNullable(COLLECTOR.taskDtoMap.get(taskId))
                            .map(WeakReference::get)
                            .ifPresent(info -> usage.setHeapMemoryUsage(usage.getHeapMemoryUsage() + GraphLayout.parseInstance(info).totalSize()));
                    final List<WeakReference<Object>> weakReferences = weakReferenceMap.get(taskId);
                    if (null == weakReferences) {
                        weakReferenceMap.remove(taskId);
                        return;
                    }
                    if (weakReferences.isEmpty()) {
                        weakReferenceMap.remove(taskId);
                        return;
                    }
                    final List<WeakReference<Object>> remove = new ArrayList<>();
                    eachTaskOnce(weakReferences, remove, usage);
                    if (!CollectionUtils.isEmpty(remove)) {
                        remove.forEach(e -> {
                            weakReferences.remove(e);
                            COLLECTOR.cacheMemoryMap.remove(e);
                        });
                    }
                });
    }


    void collectCpuUsage(List<String> filterTaskIds, Map<String, Usage> usageMap) {
        List<String> taskIds = new ArrayList<>(threadGroupMap.keySet())
                .stream()
                .filter(id -> CollectionUtils.isEmpty(filterTaskIds) || filterTaskIds.contains(id))
                .toList();
        asyncCollect(tasks -> {
            for (String taskId : taskIds) {
                CompletableFuture<Void> futureItem = CompletableFuture.runAsync(() -> {
                    Usage usage = usageMap.computeIfAbsent(taskId, k -> new Usage());
                    eachOneTask(taskId, usage);
                }, EXECUTOR_SERVICE);
                tasks.add(futureItem);
            }
        });
    }

    void eachThreadGroup(List<WeakReference<ThreadFactory>> weakReferences, List<WeakReference<ThreadFactory>> useless, LongConsumer runnable) {
        for (int i = weakReferences.size() - 1; i >= 0; i--) {
            final int index = i;
            ignore(() -> {
                final WeakReference<ThreadFactory> weakReference = weakReferences.get(index);
                final ThreadFactory threadGroup = weakReference.get();
                if (null == threadGroup) {
                    useless.add(weakReference);
                } else {
                    for (Thread thread : ThreadGroupUtil.groupThreads(threadGroup.getThreadGroup())) {
                        if (null == thread) {
                            continue;
                        }
                        long threadId = thread.getId();
                        runnable.accept(threadId);
                    }
                }
            }, "Calculate cpu usage failed, {}");
        }
    }

    void eachOneTask(String taskId, Usage usage) {
        final List<WeakReference<ThreadFactory>> weakReferences = threadGroupMap.get(taskId);
        if (null == weakReferences) {
            threadGroupMap.remove(taskId);
            return;
        }
        synchronized (weakReferences) {
            weakReferences.removeIf(weakReference -> null == weakReference.get());
        }
        List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
        final Map<Long, Long> before = new HashMap<>();
        final long now = System.currentTimeMillis();
        eachThreadGroup(weakReferences, useless, threadId -> before.put(threadId, THREAD_CPU_TIME.getThreadCpuTime(threadId)));
        final long lead = System.currentTimeMillis() - now;
        final long last = 1000L - lead;
        if (last > 0L) {
            try {
                Thread.sleep(last);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (!useless.isEmpty()) {
            synchronized (weakReferences) {
                useless.forEach(weakReferences::remove);
            }
        }
        final long interval = Math.max(1000L, lead);
        useless = new ArrayList<>();
        eachThreadGroup(weakReferences, useless, threadId -> {
            double cpuTime = THREAD_CPU_TIME.calculateCpuUsage(threadId, before.get(threadId), interval);
            usage.addCpu(cpuTime);
        });
        if (!useless.isEmpty()) {
            synchronized (weakReferences) {
                useless.forEach(weakReferences::remove);
            }
        }
        if (weakReferences.isEmpty()) {
            threadGroupMap.remove(taskId);
        }
    }

    public static Map<String, Usage> collectOnce(List<String> taskIds) {
        final Map<String, Usage> usageMap = new HashMap<>();
        final CompletableFuture<Void> futureCpu = CompletableFuture.runAsync(() -> COLLECTOR.collectCpuUsage(taskIds, usageMap));
        final CompletableFuture<Void> futureMemory = CompletableFuture.runAsync(() -> COLLECTOR.collectMemoryUsage(taskIds, usageMap));
        try {
            futureCpu.get();
            futureMemory.get();
        } catch (InterruptedException e) {
            log.error("Collect cpu and memory usage failed InterruptedException, {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException ex) {
            log.error("Collect cpu and memory usage failed ExecutionException, {}", ex.getMessage(), ex);
        }
        return usageMap;
    }


    void ignore(Runnable runnable, String msg) {
        try {
            runnable.run();
        } catch (Exception e) {
            log.warn(msg, e.getMessage(), e);
        }
    }

    static void asyncCollect(Consumer<List<CompletableFuture<Void>>> runnable) {
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            runnable.accept(futures);
        } finally {
            for (CompletableFuture<Void> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    log.error("Collect cpu and memory usage failed ExecutionException, {}", e.getMessage(), e);
                } catch (InterruptedException e) {
                    log.error("Collect cpu and memory usage failed InterruptedException, {}", e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

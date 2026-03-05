package io.tapdata.threadgroup;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.Usage;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.core.executor.ThreadFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import io.tapdata.threadgroup.utils.FixedConcurrentHashMap;
import io.tapdata.threadgroup.utils.ThreadGroupUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.openjdk.jol.info.GraphLayout;
import org.springframework.util.CollectionUtils;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/25 10:02 Create
 * @description
 */
@Slf4j
public class CpuMemoryCollector {
    public static final long MAX_LISTENING_SIZE = Runtime.getRuntime().maxMemory() / (40L * 5L);// 25_000_000; // max allow weak ref of 1G
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

    final Map<String, FixedConcurrentHashMap<WeakReference<Object>, Long>> weakReferenceMap = new HashMap<>(16);
    final Map<String, ReferenceQueue<Object>> referenceQueue = new HashMap<>();

    final Map<String, CopyOnWriteArrayList<WeakReference<ThreadFactory>>> threadGroupMap = new ConcurrentHashMap<>(16);

    final Map<String, AtomicBoolean> cleaned = new HashMap<>();
    private volatile boolean doCollect = true;

    public static void switchChange(boolean val) {
        if (COLLECTOR.doCollect != val) {
            COLLECTOR.doCollect = val;
        }
    }

    private CpuMemoryCollector() {

    }


    public static void startTask(TaskDto taskDto) {
        String taskId = taskDto.getId().toHexString();
        try {
            COLLECTOR.taskDtoMap.put(taskId, new WeakReference<>(taskDto));
        } catch (Exception e) {
            log.warn("StartTask and register task failed, task id = {}, e = {}", taskDto.getId(), e.getMessage());
        }
    }

    public static void addNode(String taskId, String nodeId) {
        try {
            if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(taskId)) {
                return;
            }
            COLLECTOR.taskWithNode.put(nodeId, taskId);
        } catch (Exception e) {
            log.warn("Add node failed, task id = {}, node id = {}, e = {}", taskId, nodeId, e.getMessage());
        }
    }

    public static void registerTask(String nodeId, ThreadFactory threadGroup) {
        if (null == threadGroup) {
            return;
        }
        final String taskId = COLLECTOR.taskWithNode.get(nodeId);
        if (StringUtils.isEmpty(taskId)) {
            return;
        }
        try {
            COLLECTOR.referenceQueue.put(taskId, new ReferenceQueue<>());
            COLLECTOR.weakReferenceMap.put(taskId, new FixedConcurrentHashMap<>(MAX_LISTENING_SIZE));
            final CopyOnWriteArrayList<WeakReference<ThreadFactory>> weakReferences = COLLECTOR.threadGroupMap.computeIfAbsent(taskId, k -> new CopyOnWriteArrayList<>());
            weakReferences.removeIf(weakReference -> null == weakReference.get());
            for (WeakReference<ThreadFactory> weakReference : weakReferences) {
                if (weakReference.get() == threadGroup) {
                    return;
                }
            }
            weakReferences.add(new WeakReference<>(threadGroup));
        } catch (Exception e) {
            log.warn("Register task failed, node id = {}, e = {}", nodeId, e.getMessage());
        } finally {
            COLLECTOR.startClean(taskId);
        }
    }


    void startClean(String taskId) {
        AtomicBoolean cleanTag = COLLECTOR.cleaned.computeIfAbsent(taskId, k -> new AtomicBoolean(false));
        if (cleanTag.get()) {
            return;
        }
        cleanTag.compareAndSet(false, true);
        EXECUTOR_SERVICE.submit(() -> {
            try {
                while (COLLECTOR.cleaned.containsKey(taskId) && cleanTag.get()) {
                    FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferenceLongHashMap = weakReferenceMap.get(taskId);
                    ReferenceQueue<Object> taskReferenceQueue = referenceQueue.get(taskId);
                    if (null == weakReferenceLongHashMap || null == taskReferenceQueue) {
                        try {
                            Thread.yield();
                        } catch (Exception e) {
                            // ignore
                        }
                        continue;
                    }
                    try {
                        WeakReference<Object> pull;
                        do {
                            pull = (WeakReference<Object>) taskReferenceQueue.remove(500L);
                            weakReferenceLongHashMap.remove(pull);
                        } while (null != pull && cleanTag.get());
                    } catch (IllegalArgumentException e) {
                        //ignore
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                }
            } finally {
                cleanTag.compareAndSet(true, false);
            }
        });
    }

    public static void unregisterTask(String taskId) {
        AtomicBoolean cleanTag = COLLECTOR.cleaned.get(taskId);
        if (cleanTag != null) {
            cleanTag.set(false);
        }
        CommonUtils.handleAnyError(() -> COLLECTOR.threadGroupMap.remove(taskId),
                e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean threadGroupMap, {}", taskId, e.getMessage()));
        synchronized (COLLECTOR.weakReferenceMap) {
            CommonUtils.handleAnyError(() -> COLLECTOR.weakReferenceMap.remove(taskId),
                    e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean weakReferenceMap of task, {}", taskId, e.getMessage()));
        }
        CommonUtils.handleAnyError(() -> COLLECTOR.taskDtoMap.remove(taskId),
                e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean taskDtoMap, {}", taskId, e.getMessage()));
        CommonUtils.handleAnyError(() -> {
                    final List<String> nodeIds = new ArrayList<>(COLLECTOR.taskWithNode.keySet());
                    nodeIds.forEach(nodeId -> {
                        if (Objects.equals(taskId, COLLECTOR.taskWithNode.get(nodeId))) {
                            COLLECTOR.taskWithNode.remove(nodeId);
                        }
                    });
                },
                e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean taskWithNode, {}", taskId, e.getMessage()));
        CommonUtils.handleAnyError(() -> {
                    COLLECTOR.referenceQueue.remove(taskId);
                },
                e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean referenceQueue, {}", taskId, e.getMessage()));
        CommonUtils.handleAnyError(() -> {
                    COLLECTOR.cleaned.remove(taskId);
                },
                e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean cleaned, {}", taskId, e.getMessage()));
    }

    public static void listeningTables(String nodeId, TapTableMap<?,?> iTable) {
        if (!COLLECTOR.doCollect) {
            return;
        }
        List<TapTable> tapTables = new ArrayList<>();
        final String taskId = COLLECTOR.taskWithNode.get(nodeId);
        iTable.keySet().forEach(k -> tapTables.add(iTable.get(k)));
        if (Objects.isNull(taskId)) {
            return;
        }
        ReferenceQueue<Object> taskReferenceQueue = COLLECTOR.referenceQueue.get(taskId);
        if (Objects.isNull(taskReferenceQueue)) {
            return;
        }
        FixedConcurrentHashMap<WeakReference<Object>, Long> weakMap = COLLECTOR.weakReferenceMap.get(taskId);
        if (Objects.isNull(weakMap)) {
            return;
        }
        try {
            weakMap.put(new WeakReference<>(tapTables, taskReferenceQueue), -1L);
        } catch (Exception e) {
            log.warn("Listening tables failed, node id = {}, e = {}", nodeId, e.getMessage());
        }
    }

    public static void listening(String nodeId, Object info) {
        if (!COLLECTOR.doCollect) {
            return;
        }
        if (!(info instanceof TapEvent)
                && !(info instanceof TapdataEvent)
                && !(info instanceof Map<?, ?>)
                && !(info instanceof Collection<?>)) {
            return;
        }
        final String taskId = COLLECTOR.taskWithNode.get(nodeId);
        if (null == taskId) {
            return;
        }
        ReferenceQueue<Object> taskReferenceQueue = COLLECTOR.referenceQueue.get(taskId);
        if (null == taskReferenceQueue) {
            return;
        }
        FixedConcurrentHashMap<WeakReference<Object>, Long> weakMap = COLLECTOR.weakReferenceMap.get(taskId);
        if (null == weakMap) {
            return;
        }
        try {
            weakMap.put(new WeakReference<>(info, taskReferenceQueue), -1L);
        } catch (Exception e) {
            log.warn("Listening failed, node id = {}, e = {}", nodeId, e.getMessage());
        }
    }

    void eachTaskOnce(FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferences, Usage usage) {
        Iterator<Map.Entry<WeakReference<Object>, Long>> iterator = weakReferences.entrySet().iterator();
        List<WeakReference<Object>> toUpdate = new ArrayList<>();
        Map<WeakReference<Object>, Long> updates = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<WeakReference<Object>, Long> entry = iterator.next();
            WeakReference<Object> weakRef = entry.getKey();
            Object obj = weakRef.get();
            if (obj == null) {
                iterator.remove();
                continue;
            }
            Long size = entry.getValue();
            if (size == null || size <= 0L) {
                // 延迟计算，避免阻塞遍历
                toUpdate.add(weakRef);
            } else {
                usage.setHeapMemoryUsage(usage.getHeapMemoryUsage() + size);
            }
        }
        for (WeakReference<Object> weakRef : toUpdate) {
            ignore(() -> {
                Object obj = weakRef.get();
                if (obj != null) {
                    long size = RamUsageEstimator.sizeOfObject(obj) + 40;
                    updates.put(weakRef, size);
                    usage.setHeapMemoryUsage(usage.getHeapMemoryUsage() + size);
                }
            }, "Calculate memory usage failed, {}");
        }
        weakReferences.putAll(updates);
    }

    void collectMemoryUsage(List<String> filterTaskIds, Map<String, Usage> usageMap) {
        final List<String> taskIds = new ArrayList<>(weakReferenceMap.keySet());
        taskIds.stream()
                .filter(id -> CollectionUtils.isEmpty(filterTaskIds) || filterTaskIds.contains(id))
                .filter(taskId -> {
                    FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferences = COLLECTOR.weakReferenceMap.get(taskId);
                    return !CollectionUtils.isEmpty(weakReferences);
                }).forEach(taskId -> {
                    final Usage usage = usageMap.computeIfAbsent(taskId, k -> new Usage());
                    Optional.ofNullable(COLLECTOR.taskDtoMap.get(taskId))
                            .map(WeakReference::get)
                            .ifPresent(info -> usage.setHeapMemoryUsage(usage.getHeapMemoryUsage() + GraphLayout.parseInstance(info).totalSize()));
                    final FixedConcurrentHashMap<WeakReference<Object>, Long> weakReferences = weakReferenceMap.get(taskId);
                    eachTaskOnce(weakReferences, usage);
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
        weakReferences.removeIf(weakReference -> null == weakReference.get());
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
            useless.forEach(weakReferences::remove);
        }
        final long interval = Math.max(1000L, lead);
        useless = new ArrayList<>();
        eachThreadGroup(weakReferences, useless, threadId -> {
            double cpuTime = THREAD_CPU_TIME.calculateCpuUsage(threadId, before.get(threadId), interval);
            usage.addCpu(cpuTime);
        });
        if (!useless.isEmpty()) {
            useless.forEach(weakReferences::remove);
        }
        if (weakReferences.isEmpty()) {
            threadGroupMap.remove(taskId);
        }
    }

    public static Map<String, Usage> collectOnce(List<String> taskIds) {
        final Map<String, Usage> usageMap = new HashMap<>();
        if (!COLLECTOR.doCollect) {
            COLLECTOR.stopCollect(taskIds, usageMap);
            return usageMap;
        }
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

    void stopCollect(List<String> filterTaskIds, Map<String, Usage> usageMap) {
        new ArrayList<>(threadGroupMap.keySet())
                .stream()
                .filter(id -> CollectionUtils.isEmpty(filterTaskIds) || filterTaskIds.contains(id))
                .forEach(task -> {
                    Usage usage = new Usage();
                    usage.setCpuUsage(null);
                    usage.setHeapMemoryUsage(null);
                    usageMap.put(task, usage);
                });

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

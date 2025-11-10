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
    final Map<String, Info> taskInfo = new HashMap<>();

    static class Info {
        String taskId;
        Long lastCount = 0L;

        public boolean judged(long size) {
            if (lastCount <= 0L) {
                lastCount = size;
                return true;
            }
            double rate = Math.abs(size - lastCount) * 1.0D / lastCount;
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
        Info item = new Info();
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
        String taskId = COLLECTOR.taskWithNode.get(nodeId);
        if (StringUtils.isEmpty(taskId)) {
            return;
        }
        List<WeakReference<ThreadFactory>> weakReferences = COLLECTOR.threadGroupMap.computeIfAbsent(taskId, k -> new ArrayList<>());
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
        COLLECTOR.weakReferenceMap.remove(taskId);
        COLLECTOR.taskDtoMap.remove(taskId);
        COLLECTOR.taskInfo.remove(taskId);
        List<String> nodeIds = new ArrayList<>(COLLECTOR.taskWithNode.keySet());
        nodeIds.forEach(nodeId -> {
            if (Objects.equals(taskId, COLLECTOR.taskWithNode.get(nodeId))) {
                COLLECTOR.taskWithNode.remove(nodeId);
            }
        });
    }

    public static void listening(String nodeId, Object info) {
        String taskId = COLLECTOR.taskWithNode.get(nodeId);
        if (StringUtils.isEmpty(taskId)) {
            return;
        }
        List<WeakReference<Object>> weakReferences = COLLECTOR.weakReferenceMap.computeIfAbsent(taskId, k -> new LinkedList<>());
        WeakReference<Object> reference = new WeakReference<>(info);
        weakReferences.add(reference);
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
                        Object object = weakReference.get();
                        if (null != object) {
                            long sizeOf;
                            try {
                                sizeOf = GraphLayout.parseInstance(object).totalSize();
                            } catch (Exception e) {
                                sizeOf = RamUsageEstimator.sizeOfObject(object);
                            }
                            usage.setHeapMemoryUsage(usage.getHeapMemoryUsage() + sizeOf);
                        } else {
                            remove.add(weakReference);
                        }
                    }, "Calculate memory usage failed, {}");
                });
    }

    void collectMemoryUsage(List<String> filterTaskIds, Map<String, Usage> usageMap) {
        List<String> taskIds = new ArrayList<>(weakReferenceMap.keySet());
        asyncCollect(tasks ->
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
                            return item.judged(weakReferences.size());
                        }).forEach(taskId -> {
                            CompletableFuture<Void> futureItem = CompletableFuture.runAsync(() -> {
                                Usage usage = usageMap.computeIfAbsent(taskId, k -> new Usage());
                                Optional.ofNullable(COLLECTOR.taskDtoMap.get(taskId))
                                        .map(WeakReference::get)
                                        .ifPresent(info -> usage.setHeapMemoryUsage(usage.getHeapMemoryUsage() + GraphLayout.parseInstance(info).totalSize()));
                                List<WeakReference<Object>> weakReferences = weakReferenceMap.get(taskId);
                                if (null == weakReferences) {
                                    weakReferenceMap.remove(taskId);
                                    return;
                                }
                                if (weakReferences.isEmpty()) {
                                    weakReferenceMap.remove(taskId);
                                    return;
                                }
                                List<WeakReference<Object>> remove = new ArrayList<>();
                                eachTaskOnce(weakReferences, remove, usage);
                                if (!CollectionUtils.isEmpty(remove)) {
                                    CompletableFuture<Void> removeFuture = CompletableFuture.runAsync(() -> remove.forEach(weakReferences::remove), EXECUTOR_SERVICE);
                                    tasks.add(removeFuture);
                                }
                            }, EXECUTOR_SERVICE);
                            tasks.add(futureItem);
                        })
        );
    }


    void collectCpuUsage(List<String> filterTaskIds, Map<String, Usage> usageMap) {
        List<String> taskIds = new ArrayList<>(threadGroupMap.keySet())
                .stream()
                .filter(id -> CollectionUtils.isEmpty(filterTaskIds) || filterTaskIds.contains(id))
                .toList();
        for (String taskId : taskIds) {
            Usage usage = usageMap.computeIfAbsent(taskId, k -> new Usage());
            eachOneTask(taskId, usage);
        }
    }

    void eachThreadGroup(List<WeakReference<ThreadFactory>> weakReferences, List<WeakReference<ThreadFactory>> useless, LongConsumer runnable) {
        for (int i = weakReferences.size() - 1; i >= 0; i--) {
            final int index = i;
            ignore(() -> {
                WeakReference<ThreadFactory> weakReference = weakReferences.get(index);
                ThreadFactory threadGroup = weakReference.get();
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
        List<WeakReference<ThreadFactory>> weakReferences = threadGroupMap.get(taskId);
        if (null == weakReferences) {
            threadGroupMap.remove(taskId);
            return;
        }
        synchronized (weakReferences) {
            weakReferences.removeIf(weakReference -> null == weakReference.get());
        }
        List<WeakReference<ThreadFactory>> useless = new ArrayList<>();
        Map<Long, Long> before = new HashMap<>();
        long now = System.currentTimeMillis();
        eachThreadGroup(weakReferences, useless, threadId -> before.put(threadId, THREAD_CPU_TIME.getThreadCpuTime(threadId)));
        long lead = System.currentTimeMillis() - now;
        long last = 1000L - lead;
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
        long interval = Math.max(1000L, lead);
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
        Map<String, Usage> usageMap = new HashMap<>();
        CompletableFuture<Void> futureCpu = CompletableFuture.runAsync(() -> COLLECTOR.collectCpuUsage(taskIds, usageMap));
        CompletableFuture<Void> futureMemory = CompletableFuture.runAsync(() -> COLLECTOR.collectMemoryUsage(taskIds, usageMap));
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
        List<CompletableFuture<Void>> futures = new ArrayList<>();
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

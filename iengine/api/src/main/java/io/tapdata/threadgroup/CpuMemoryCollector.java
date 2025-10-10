package io.tapdata.threadgroup;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.Usage;
import io.tapdata.pdk.core.executor.ThreadFactory;
import io.tapdata.threadgroup.utils.ThreadGroupUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.LongConsumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/25 10:02 Create
 * @description
 */
@Slf4j
public final class CpuMemoryCollector {
    public static final ThreadCPUMonitor THREAD_CPU_TIME = new ThreadCPUMonitor();
    public static final CpuMemoryCollector COLLECTOR = new CpuMemoryCollector();
    Map<String, String> taskWithNode = new HashMap<>(16);
    Map<String, WeakReference<TaskDto>> taskDtoMap = new HashMap<>(16);
    Map<String, List<WeakReference<Object>>> weakReferenceMap = new HashMap<>(16);
    Map<String, List<WeakReference<ThreadFactory>>> threadGroupMap = new HashMap<>(16);

    private CpuMemoryCollector() {

    }


    public static void startTask(TaskDto taskDto) {
        COLLECTOR.taskDtoMap.put(taskDto.getId().toHexString(), new WeakReference<>(taskDto));
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
        for (WeakReference<ThreadFactory> weakReference : weakReferences) {
            if (null == weakReference.get()) {
                weakReferences.remove(weakReference);
            }
            if (weakReference.get() == threadGroup) {
                return;
            }
        }
        weakReferences.add(new WeakReference<>(threadGroup));
    }

    public static void unregisterTask(String taskId) {
        COLLECTOR.threadGroupMap.remove(taskId);
        COLLECTOR.weakReferenceMap.remove(taskId);
        COLLECTOR.taskDtoMap.remove(taskId);
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
        List<WeakReference<Object>> weakReferences = COLLECTOR.weakReferenceMap.computeIfAbsent(taskId, k -> new ArrayList<>());
        WeakReference<Object> reference = new WeakReference<>(info);
        weakReferences.add(reference);
    }

    void collectMemoryUsage(Map<String, Usage> usageMap) {
        List<String> taskIds = new ArrayList<>(weakReferenceMap.keySet());
        taskIds.forEach(taskId -> {
            Usage usage = usageMap.computeIfAbsent(taskId, k -> new Usage());
            Optional.ofNullable(COLLECTOR.taskDtoMap.get(taskId))
                    .ifPresent(info -> usage.setHeapMemoryUsage(usage.getHeapMemoryUsage() + RamUsageEstimator.sizeOfObject(info)));
            List<WeakReference<Object>> weakReferences = weakReferenceMap.get(taskId);
            if (null == weakReferences) {
                weakReferenceMap.remove(taskId);
                return;
            }
            weakReferences.removeIf(weakReference -> null == weakReference.get());
            if (weakReferences.isEmpty()) {
                weakReferenceMap.remove(taskId);
                return;
            }
            List<WeakReference<Object>> useless = new ArrayList<>();
            for (WeakReference<Object> weakReference : weakReferences) {
                Object object = weakReference.get();
                if (null != object) {
                    usage.setHeapMemoryUsage(usage.getHeapMemoryUsage() + RamUsageEstimator.sizeOfObject(object));
                } else {
                    useless.add(weakReference);
                }
            }
            if (!useless.isEmpty()) {
                useless.forEach(weakReferences::remove);
            }
        });
    }


    void collectCpuUsage(Map<String, Usage> usageMap) {
        List<String> taskIds = new ArrayList<>(threadGroupMap.keySet());
        for (String taskId : taskIds) {
            Usage usage = usageMap.computeIfAbsent(taskId, k -> new Usage());
            eachOneTask(taskId, usage);
        }
    }

    void eachThreadGroup(List<WeakReference<ThreadFactory>> weakReferences, List<WeakReference<ThreadFactory>> useless, LongConsumer runnable) {
        for (WeakReference<ThreadFactory> weakReference : weakReferences) {
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
        }
    }

    void eachOneTask(String taskId, Usage usage) {
        List<WeakReference<ThreadFactory>> weakReferences = threadGroupMap.get(taskId);
        if (null == weakReferences) {
            threadGroupMap.remove(taskId);
            return;
        }
        weakReferences.removeIf(weakReference -> null == weakReference.get());
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
            useless.forEach(weakReferences::remove);
        }
        long interval = Math.max(1000L, lead);
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

    public static Map<String, Usage> collectOnce() {
        Map<String, Usage> usageMap = new HashMap<>();
        CompletableFuture<Void> futureCpu = CompletableFuture.runAsync(() -> COLLECTOR.collectMemoryUsage(usageMap));
        CompletableFuture<Void> futureMemory = CompletableFuture.runAsync(() -> COLLECTOR.collectCpuUsage(usageMap));
        try {
            CompletableFuture.allOf(futureCpu, futureMemory).get();
        } catch (InterruptedException e) {
            log.error("Collect cpu and memory usage failed InterruptedException, {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException ex) {
            log.error("Collect cpu and memory usage failed ExecutionException, {}", ex.getMessage(), ex);
        }
        return usageMap;
    }
}

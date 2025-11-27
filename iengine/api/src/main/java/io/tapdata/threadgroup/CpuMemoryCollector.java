package io.tapdata.threadgroup;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.Usage;
import io.tapdata.pdk.core.executor.ThreadFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.threadgroup.utils.ThreadGroupUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.openjdk.jol.info.GraphLayout;
import org.springframework.util.CollectionUtils;

import java.lang.ref.WeakReference;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
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
	final Map<String, String> taskWithNode = new ConcurrentHashMap<>(16);
	final Map<String, WeakReference<TaskDto>> taskDtoMap = new ConcurrentHashMap<>(16);

	final Map<String, List<WeakReference<Object>>> weakReferenceMap = new HashMap<>(16);

	final Map<String, List<WeakReference<Object>>> cacheLeftWeakReferenceMap = new HashMap<>(16);
	final Map<String, List<WeakReference<Object>>> cacheRightWeakReferenceMap = new HashMap<>(16);

	final Map<String, CopyOnWriteArrayList<WeakReference<ThreadFactory>>> threadGroupMap = new ConcurrentHashMap<>(16);

	final Map<String, Info> taskInfo = new ConcurrentHashMap<>(16);

	private volatile boolean doCollect = true;
	public static void switchChange(boolean val) {
		if (COLLECTOR.doCollect != val) {
			COLLECTOR.doCollect = val;
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

	public static void cleanOnce() {
		List<String> taskIds = new ArrayList<>(COLLECTOR.taskDtoMap.keySet());
		for (String taskId : taskIds) {
			final List<WeakReference<Object>> list;
			synchronized (COLLECTOR.weakReferenceMap) {
				list = COLLECTOR.weakReferenceMap.computeIfAbsent(taskId, key -> new ArrayList<>());
				list.removeIf(e -> Objects.isNull(e.get()));
			}
			final int sec = LocalDateTime.now().getSecond();
			int step = sec / 5;
			int type = step % 2;
			if (type == 0) {
				clean(COLLECTOR.cacheLeftWeakReferenceMap, taskId, list);
			} else {
				clean(COLLECTOR.cacheRightWeakReferenceMap, taskId, list);
			}
		}
	}

	static void clean(Map<String, List<WeakReference<Object>>> map, String taskId, List<WeakReference<Object>> list) {
		List<WeakReference<Object>> listRight = map.get(taskId);
		if (CollectionUtils.isEmpty(listRight)) {
			return;
		}
		synchronized (listRight) {
			listRight.removeIf(e -> Objects.isNull(e.get()));
			if (!CollectionUtils.isEmpty(listRight)) {
				list.addAll(listRight);
				listRight.clear();
			}
		}
	}


	public static void startTask(TaskDto taskDto) {
		final Info item = new Info();
		item.taskId = taskDto.getId().toHexString();
		try {
			COLLECTOR.taskDtoMap.put(item.taskId, new WeakReference<>(taskDto));
			COLLECTOR.taskInfo.put(item.taskId, item);
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
		try {
			if (null == threadGroup) {
				return;
			}
			final String taskId = COLLECTOR.taskWithNode.get(nodeId);
			if (StringUtils.isEmpty(taskId)) {
				return;
			}
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
		}
	}


	public static void unregisterTask(String taskId) {
		CommonUtils.handleAnyError(() -> COLLECTOR.threadGroupMap.remove(taskId),
				e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean threadGroupMap, {}", taskId, e.getMessage()));
		synchronized (COLLECTOR.weakReferenceMap) {
			CommonUtils.handleAnyError(() -> COLLECTOR.weakReferenceMap.remove(taskId),
					e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean weakReferenceMap of task, {}", taskId, e.getMessage()));
		}
		synchronized (COLLECTOR.cacheLeftWeakReferenceMap) {
			CommonUtils.handleAnyError(() -> COLLECTOR.cacheLeftWeakReferenceMap.remove(taskId),
					e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean weakReferenceMap of task, {}", taskId, e.getMessage()));
		}
		synchronized (COLLECTOR.cacheRightWeakReferenceMap) {
			CommonUtils.handleAnyError(() -> COLLECTOR.cacheRightWeakReferenceMap.remove(taskId),
					e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean weakReferenceMap of task, {}", taskId, e.getMessage()));
		}
		CommonUtils.handleAnyError(() -> COLLECTOR.taskDtoMap.remove(taskId),
				e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean taskDtoMap, {}", taskId, e.getMessage()));
		CommonUtils.handleAnyError(() -> COLLECTOR.taskInfo.remove(taskId),
				e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean taskInfo, {}", taskId, e.getMessage()));
		CommonUtils.handleAnyError(() -> {
					final List<String> nodeIds = new ArrayList<>(COLLECTOR.taskWithNode.keySet());
					nodeIds.forEach(nodeId -> {
						if (Objects.equals(taskId, COLLECTOR.taskWithNode.get(nodeId))) {
							COLLECTOR.taskWithNode.remove(nodeId);
						}
					});
				},
				e -> log.warn("Unregister task {} from cpu memory collector failed: can not clean taskWithNode, {}", taskId, e.getMessage()));

	}

	public static void listening(String nodeId, Object info) {
		if (!COLLECTOR.doCollect) {
			return;
		}
		if (null == info) {
			return;
		}
		try {
			final String taskId = COLLECTOR.taskWithNode.get(nodeId);
			if (StringUtils.isEmpty(taskId)) {
				return;
			}
			final WeakReference<Object> reference = new WeakReference<>(info);
			final int sec = LocalDateTime.now().getSecond();
			int step = sec / 5;
			int type = step % 2;
			final List<WeakReference<Object>> weakReferences = type == 0 ? COLLECTOR.cacheRightWeakReferenceMap.computeIfAbsent(taskId, k -> new ArrayList<>()) :
					COLLECTOR.cacheLeftWeakReferenceMap.computeIfAbsent(taskId, k -> new ArrayList<>());
			synchronized (weakReferences) {
				weakReferences.add(reference);
			}
		} catch (Exception e) {
			log.warn("Listening failed, node id = {}, e = {}", nodeId, e.getMessage());
		}
	}

	void eachTaskOnce(List<WeakReference<Object>> weakReferences, Usage usage) {
		weakReferences.stream()
				.filter(Objects::nonNull)
				.forEach(weakReference -> {
					if (null == weakReference.get()) {
						return;
					}
					ignore(() -> {
						Object o = weakReference.get();
						if (null == o) {
							return;
						}
						long sizeOf;
						try {
							sizeOf = GraphLayout.parseInstance().totalSize();
						} catch (Exception e) {
							sizeOf = RamUsageEstimator.sizeOfObject(o);
						}
						final long value = usage.getHeapMemoryUsage() + sizeOf;
						usage.setHeapMemoryUsage(value);
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
					return item.judged(weakReferences.size());
				}).forEach(taskId -> {
					final Usage usage = usageMap.computeIfAbsent(taskId, k -> new Usage());
					Optional.ofNullable(COLLECTOR.taskDtoMap.get(taskId))
							.map(WeakReference::get)
							.ifPresent(info -> usage.setHeapMemoryUsage(usage.getHeapMemoryUsage() + GraphLayout.parseInstance(info).totalSize()));
					final List<WeakReference<Object>> weakReferences = weakReferenceMap.get(taskId);
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

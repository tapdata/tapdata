package io.tapdata.flow.engine.V2.node.hazelcast.data.concurrent;

import com.tapdata.tm.commons.dag.Node;
import org.apache.commons.collections4.ListUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author samuel
 * @Description
 * @create 2024-07-18 18:05
 **/
public class ProcessorNodeConcurrentExecutor {
	public static final String TAG = ProcessorNodeConcurrentExecutor.class.getSimpleName();
	protected final Node node;
	protected final int threadNum;
	protected final ThreadPoolExecutor processThreadPool;

	public ProcessorNodeConcurrentExecutor(Node node, int threadNum) {
		this.node = node;
		this.threadNum = threadNum;
		this.processThreadPool = new ThreadPoolExecutor(
				0, threadNum + 1, 10L, TimeUnit.SECONDS,
				new SynchronousQueue<>(),
				new ThreadFactory() {
					final AtomicInteger index = new AtomicInteger();

					@Override
					public Thread newThread(@NotNull Runnable r) {
						return new Thread(r, String.join("_", TAG, node.getType(), node.getId(), index.getAndIncrement() + ""));
					}
				}
		);
	}

	public <E, R> List<R> submit(List<E> events, Function<List<E>, List<R>> eventsFunction) {
		int partitionSize = Math.max(100, events.size() / threadNum);
		List<List<E>> partitioned = ListUtils.partition(events, partitionSize);
		List<CompletableFuture<ThreadEvent<R>>> completableFutures = new ArrayList<>();
		List<ThreadEvent<R>> processedThreadEvents = new ArrayList<>();
		List<R> results = new ArrayList<>();
		for (int i = 0; i < partitioned.size(); i++) {
			List<E> partitionEvents = partitioned.get(i);
			ThreadEvent<E> threadEvent = new ThreadEvent<>(i, partitionEvents);
			completableFutures.add(
					CompletableFuture.completedFuture(threadEvent).thenApplyAsync(t -> {
						List<R> apply = eventsFunction.apply(t.getEvents());
						return new ThreadEvent<>(t.getIndex(), apply);
					}, processThreadPool)
			);
		}
		for (CompletableFuture<ThreadEvent<R>> completableFuture : completableFutures) {
			try {
				processedThreadEvents.add(completableFuture.get());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
		}
		processedThreadEvents.stream().sorted(Comparator.comparing(ThreadEvent::getIndex)).forEach(t -> results.addAll(t.getEvents()));
		return results;
	}

	public static class ThreadEvent<T> {
		private final Integer index;
		private final List<T> events;

		public ThreadEvent(Integer index, List<T> events) {
			if (null == index) {
				throw new IllegalArgumentException("index cannot be null");
			}
			this.index = index;
			this.events = events;
		}

		public Integer getIndex() {
			return index;
		}

		public List<T> getEvents() {
			return events;
		}
	}

	public void close() {
		Optional.ofNullable(processThreadPool).ifPresent(ThreadPoolExecutor::shutdownNow);
	}
}

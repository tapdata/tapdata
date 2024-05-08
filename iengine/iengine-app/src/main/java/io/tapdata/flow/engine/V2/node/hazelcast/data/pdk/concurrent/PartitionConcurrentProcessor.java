package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent;

import com.google.common.collect.Queues;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.error.EngineExCode_33;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.PartitionResult;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.Partitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.PartitionKeySelector;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author jackin
 * @date 2022/7/25 16:42
 **/
public class PartitionConcurrentProcessor {

	public static final String PROCESS_QUEUE_IF_FULL_WAITING_FOR_ENQUEUE_MESSAGE = "process queue if full, waiting for enqueue";
	private final String concurrentProcessThreadNamePrefix;

	private static final String LOG_PREFIX = "[partition concurrent] ";

	private static final int DEFAULT_PARTITION = 0;

	private final Logger logger = LogManager.getLogger(PartitionConcurrentProcessor.class);

	protected final ExecutorService executorService;

	protected final List<LinkedBlockingQueue<PartitionEvent<TapdataEvent>>> partitionsQueue;

	private final int partitionSize;
	private final int batchSize;

	private final AtomicBoolean currentRunning = new AtomicBoolean(false);

	private final Consumer<List<TapdataEvent>> eventProcessor;

	private final Partitioner<TapdataEvent, List<Object>> partitioner;

	private final PartitionKeySelector<TapEvent, Object, Map<String, Object>> keySelector;

	private final AtomicLong eventSeq = new AtomicLong(0L);

	private final LinkedBlockingQueue<WatermarkEvent> watermarkQueue;

	private final Consumer<TapdataEvent> flushOffset;
	private final ErrorHandler<Throwable, String> errorHandler;
	private final Supplier<Boolean> nodeRunning;
	private final TaskDto taskDto;

	public PartitionConcurrentProcessor(
		int partitionSize,
		int batchSize,
		Partitioner<TapdataEvent, List<Object>> partitioner,
		PartitionKeySelector<TapEvent, Object, Map<String, Object>> keySelector,
		Consumer<List<TapdataEvent>> eventProcessor,
		Consumer<TapdataEvent> flushOffset,
		ErrorHandler<Throwable, String> errorHandler,
		Supplier<Boolean> nodeRunning,
		TaskDto taskDto
	) {

		this.concurrentProcessThreadNamePrefix = "concurrent-process-thread-" + taskDto.getId().toHexString() + "-" + taskDto.getName() + "-";

		this.taskDto = taskDto;
		this.batchSize = batchSize;

		this.partitionSize = partitionSize;

		this.executorService = new ThreadPoolExecutor(partitionSize + 1, partitionSize + 1,
			60L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<>(1)
		);
		logger.info(LOG_PREFIX + "completed create thread pool, pool size {}", partitionSize + 1);

		this.errorHandler = errorHandler;
		this.nodeRunning = nodeRunning;
		this.partitionsQueue = IntStream
			.range(0, partitionSize)
			.mapToObj(
				i -> new LinkedBlockingQueue<PartitionEvent<TapdataEvent>>(batchSize * 2)
			).collect(Collectors.toList());

		watermarkQueue = new LinkedBlockingQueue<>(batchSize);

		this.eventProcessor = eventProcessor;

		currentRunning.compareAndSet(false, true);

		Assert.notNull(partitioner, () -> LOG_PREFIX + "partitioner cannot be null.");
		this.partitioner = partitioner;
		Assert.notNull(keySelector, () -> LOG_PREFIX + "keySelector cannot be null.");
		this.keySelector = keySelector;
		this.flushOffset = flushOffset;
		this.executorService.submit(this::watermarkEventRunner);
	}

	public void start() {
		for (int partition = 0; partition < partitionSize; partition++) {
			final LinkedBlockingQueue<PartitionEvent<TapdataEvent>> linkedBlockingQueue = partitionsQueue.get(partition);
			int finalPartition = partition;
			executorService.submit(() -> partitionConsumer(finalPartition, linkedBlockingQueue));
		}
	}

	protected WatermarkEvent pollWatermarkEvent() throws InterruptedException {
		return watermarkQueue.poll(3, TimeUnit.SECONDS);
	}

	protected void watermarkEventRunner() {
		while (isRunning()) {
			Thread.currentThread().setName(taskDto.getId().toHexString() + "-" + taskDto.getName() + "-watermark-event-process");
			try {
				final WatermarkEvent watermarkEvent = pollWatermarkEvent();
				if (watermarkEvent != null) {
					final CountDownLatch countDownLatch = watermarkEvent.getCountDownLatch();
					final TapdataEvent event = watermarkEvent.getEvent();
					if (!waitCountDownLath(countDownLatch, () -> {
						final Date sourceTime = Optional.ofNullable(event.getSourceTime()).map(Date::new).orElse(null);
						logger.info("waiting watermark event for all thread process, ts {}", sourceTime);
					})) return; // when task stop, do not need flush offset
					this.flushOffset.accept(event);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			} catch (Throwable throwable) {
				currentRunning.compareAndSet(true, false);
				errorHandler.accept(throwable, "process watermark event failed");
			} finally {
				ThreadContext.clearAll();
			}
		}
	}

	protected void partitionConsumer(int finalPartition, LinkedBlockingQueue<PartitionEvent<TapdataEvent>> linkedBlockingQueue) {
		try {
			Thread.currentThread().setName(concurrentProcessThreadNamePrefix + finalPartition);
			List<TapdataEvent> processEvents = new ArrayList<>();
			while (isRunning()) {
				try {
					List<PartitionEvent<TapdataEvent>> events = new ArrayList<>();
					Queues.drain(linkedBlockingQueue, events, batchSize, 3, TimeUnit.SECONDS);
					if (events.isEmpty()) continue;

					processPartitionEvents(finalPartition, processEvents, events);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				} catch (Throwable throwable) {
					currentRunning.compareAndSet(true, false);
					errorHandler.accept(throwable, "target write record(s) failed");
				}
			}
		} finally {
			ThreadContext.clearAll();
		}
	}

	protected void processPartitionEvents(int finalPartition, List<TapdataEvent> processEvents, List<PartitionEvent<TapdataEvent>> events) throws InterruptedException {
		for (PartitionEvent partitionEvent : events) {
			if (partitionEvent instanceof NormalEvent) {
				final NormalEvent<?> normalEvent = (NormalEvent<?>) partitionEvent;
				final TapdataEvent event = (TapdataEvent) normalEvent.getEvent();
				processEvents.add(event);
			} else if (partitionEvent instanceof WatermarkEvent) {
				if (CollectionUtils.isNotEmpty(processEvents)) {
					eventProcessor.accept(processEvents);
					processEvents.clear();
				}
				final CountDownLatch countDownLatch = ((WatermarkEvent) partitionEvent).getCountDownLatch();
				countDownLatch.countDown();
			} else {
				if (CollectionUtils.isNotEmpty(processEvents)) {
					eventProcessor.accept(processEvents);
					processEvents.clear();
				}
				final CountDownLatch countDownLatch = ((BarrierEvent) partitionEvent).getCountDownLatch();
				countDownLatch.countDown();

				waitCountDownLath(countDownLatch, () -> logger.debug(wrapPartitionErrorMsg(finalPartition, "process completed, waiting other thread completed.")));
			}
		}

		if (CollectionUtils.isNotEmpty(processEvents)) {
			eventProcessor.accept(processEvents);
			processEvents.clear();
		}
	}

	protected String wrapPartitionErrorMsg(int partition, String msg) {
		return LOG_PREFIX + "thread-" + partition + " " + msg;
	}

	protected boolean toSingleMode(TapEvent tapEvent, List<Object> partitionValue, AtomicBoolean singleMode) throws InterruptedException {
		// 如果遇到，删除事件&&关联键有空值，切换成单线程模式
		if (tapEvent instanceof TapDeleteRecordEvent) {
			for (Object o : partitionValue) {
				if (null != o) continue;

				if (singleMode.compareAndSet(false, true)) {
					generateBarrierEvent();
				}
				return true;
			}
		}

		if (singleMode.compareAndSet(true, false)) {
			generateBarrierEvent();
		}
		return false;
	}

	public void process(List<TapdataEvent> tapdataEvents, boolean async) {
		if (CollectionUtils.isEmpty(tapdataEvents)) return;

		try {
			AtomicBoolean singleMode = new AtomicBoolean(false);
            TapdataEvent offsetEvent = null;
			for (TapdataEvent tapdataEvent : tapdataEvents) {
				if (!isRunning()) {
					break;
				}
				if (tapdataEvent.isDML()) {
					processDML(tapdataEvent, singleMode);
				} else if (tapdataEvent.isDDL()) {
					processDDL(tapdataEvent);
				} else {
					if (tapdataEvent.isConcurrentWrite()) {
						processSignalConcurrent(tapdataEvent);
					} else {
						processSignalWithWait(tapdataEvent);
					}
				}
                if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
                    offsetEvent = tapdataEvent;
                }
			}
			if (null != offsetEvent) {
				generateWatermarkEvent(offsetEvent);
			}

			if (!async) {
				waitingForProcessToCurrent();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	protected void processDML(TapdataEvent tapdataEvent, AtomicBoolean singleMode) throws InterruptedException {
		String tableName = "";
		try {
			final TapEvent tapEvent = tapdataEvent.getTapEvent();
			Map<String, Object> row = getTapRecordEventData(tapEvent);
			// when tapEvent is not TapRecordEvent type then 'getTapRecordEventData' throws exception
			tableName = ((TapRecordEvent) tapEvent).getTableId();

			final int partition;
			final List<Object> partitionValue = keySelector.select(tapEvent, row);
			if (toSingleMode(tapEvent, partitionValue, singleMode)) {
				partition = 0;
			} else {
				final PartitionResult<TapdataEvent> partitionResult = partitioner.partition(partitionSize, tapdataEvent, partitionValue);
				partition = partitionResult.getPartition() < 0 ? DEFAULT_PARTITION : partitionResult.getPartition();
			}

			final LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue = partitionsQueue.get(partition);
			final NormalEvent<TapdataEvent> normalEvent = new NormalEvent<>(eventSeq.incrementAndGet(), tapdataEvent);
			offer2QueueIfRunning(queue, normalEvent, wrapPartitionErrorMsg(partition, PROCESS_QUEUE_IF_FULL_WAITING_FOR_ENQUEUE_MESSAGE));
		} catch (InterruptedException e) {
			throw e;
		} catch (Exception e) {
			String msg = String.format(" tableName: %s, %s", tableName, e.getMessage());
			throw new RuntimeException(msg, e);
		}
	}

	protected void processDDL(TapdataEvent tapdataEvent) throws InterruptedException {
		generateBarrierEvent();
		final NormalEvent<TapdataEvent> normalEvent = new NormalEvent<>(eventSeq.incrementAndGet(), tapdataEvent);
		offer2QueueIfRunning(partitionsQueue.get(DEFAULT_PARTITION), normalEvent, wrapPartitionErrorMsg(DEFAULT_PARTITION, PROCESS_QUEUE_IF_FULL_WAITING_FOR_ENQUEUE_MESSAGE));
		waitingForProcessToCurrent();
	}

	protected void processSignalConcurrent(TapdataEvent tapdataEvent) throws InterruptedException {
		LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue = partitionsQueue.get(DEFAULT_PARTITION);
		NormalEvent<TapdataEvent> normalEvent = new NormalEvent<>(eventSeq.incrementAndGet(), tapdataEvent);
		offer2QueueIfRunning(queue, normalEvent, wrapPartitionErrorMsg(0, PROCESS_QUEUE_IF_FULL_WAITING_FOR_ENQUEUE_MESSAGE));
	}

	protected void processSignalWithWait(TapdataEvent tapdataEvent) throws InterruptedException {
		generateBarrierEvent();
		LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue = partitionsQueue.get(DEFAULT_PARTITION);
		NormalEvent<TapdataEvent> normalEvent = new NormalEvent<>(eventSeq.incrementAndGet(), tapdataEvent);
		offer2QueueIfRunning(queue, normalEvent, wrapPartitionErrorMsg(0, PROCESS_QUEUE_IF_FULL_WAITING_FOR_ENQUEUE_MESSAGE));
		waitingForProcessToCurrent();
	}

	protected Map<String, Object> getTapRecordEventData(TapEvent tapEvent) throws InterruptedException {
		if (tapEvent instanceof TapInsertRecordEvent) {
			return ((TapInsertRecordEvent) tapEvent).getAfter();
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			return ((TapDeleteRecordEvent) tapEvent).getBefore();
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) tapEvent;
			// if update partition value, will generate barrier event
			if (updatePartitionValueEvent(updateRecordEvent)) {
				generateBarrierEvent();
				return updateRecordEvent.getBefore();
			} else {
				return updateRecordEvent.getAfter();
			}
		}

		throw new TapCodeException(EngineExCode_33.NOT_SUPPORT_RECORD_EVENT_TYPE_EXCEPTION, "Not support TapRecordEvent type: " + tapEvent.getClass().getName());
	}

	protected boolean waitCountDownLath(CountDownLatch countDownLatch, Runnable traceOfEnabled) throws InterruptedException {
		while (!countDownLatch.await(3, TimeUnit.SECONDS)) {
			if (!isRunning()) return false;

			if (logger.isTraceEnabled()) {
				traceOfEnabled.run();
			}
		}
		return isRunning();
	}

	protected void waitingForProcessToCurrent() throws InterruptedException {
		final BarrierEvent barrierEvent = generateBarrierEvent();
		if (null == barrierEvent) return;

		final CountDownLatch countDownLatch = barrierEvent.getCountDownLatch();
		waitCountDownLath(countDownLatch, () -> logger.trace(LOG_PREFIX + "waiting for all events processed for thread"));
	}

	protected <T> void offer2QueueIfRunning(LinkedBlockingQueue<T> queue, T value, String errMsg) throws InterruptedException {
		while (isRunning() && !queue.offer(value, 3, TimeUnit.SECONDS)) {
			if (logger.isTraceEnabled()) {
				logger.trace(errMsg);
			}
		}
	}

	protected void generateWatermarkEvent(TapdataEvent tapdataEvent) throws InterruptedException {
		if (CollectionUtils.isNotEmpty(partitionsQueue)) {
			final WatermarkEvent watermarkEvent = new WatermarkEvent(partitionSize, tapdataEvent);
			addToAllPartitions(watermarkEvent, "watermark");
			offer2QueueIfRunning(watermarkQueue, watermarkEvent, LOG_PREFIX + "watermark queue is full when generate watermark event to queue.");
		}
	}

	protected BarrierEvent generateBarrierEvent() throws InterruptedException {
		if (CollectionUtils.isNotEmpty(partitionsQueue)) {
			final BarrierEvent barrierEvent = new BarrierEvent(partitionSize);
			addToAllPartitions(barrierEvent, "barrier");
			return barrierEvent;
		}

		return null;
	}

	protected void addToAllPartitions(PartitionEvent<TapdataEvent> event, String type) throws InterruptedException {
		for (int i = 0; i < partitionsQueue.size(); i++) {
			final LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue = partitionsQueue.get(i);
			offer2QueueIfRunning(queue, event, wrapPartitionErrorMsg(i, "queue is full when generate " + type + " event to queue."));
		}
	}

	public boolean isRunning() {
		return currentRunning.get() && nodeRunning.get();
	}

	public void stop() {
		try {
			waitingForProcessToCurrent();
		} catch (InterruptedException ignored) {
			Thread.currentThread().interrupt();
		} finally {
			currentRunning.compareAndSet(true, false);
			ExecutorUtil.shutdown(this.executorService, 60L, TimeUnit.SECONDS);
		}
	}


	public void forceStop() {
		currentRunning.compareAndSet(true, false);
		this.executorService.shutdownNow();
	}

	protected boolean updatePartitionValueEvent(TapUpdateRecordEvent updateRecordEvent) {
		List<Object> beforeValue = null;
		final Map<String, Object> before = updateRecordEvent.getBefore();
		if (MapUtils.isNotEmpty(before)) {
			beforeValue = keySelector.select(updateRecordEvent, before);
		}
		List<Object> afterValue = null;
		final Map<String, Object> after = updateRecordEvent.getAfter();
		if (MapUtils.isNotEmpty(before)) {
			afterValue = keySelector.select(updateRecordEvent, after);
		}
		if (beforeValue != null && afterValue != null) {
			return Objects.hash(beforeValue) != Objects.hash(afterValue);
		}

		return false;
	}

	@FunctionalInterface
	public interface ErrorHandler<T, M> {

		/**
		 * Performs this operation on the given argument.
		 *
		 * @param t the input argument
		 */
		void accept(T t, M m);

		/**
		 * Returns a composed {@code Consumer} that performs, in sequence, this
		 * operation followed by the {@code after} operation. If performing either
		 * operation throws an exception, it is relayed to the caller of the
		 * composed operation.  If performing this operation throws an exception,
		 * the {@code after} operation will not be performed.
		 *
		 * @param after the operation to perform after this operation
		 * @return a composed {@code Consumer} that performs in sequence this
		 * operation followed by the {@code after} operation
		 * @throws NullPointerException if {@code after} is null
		 */
		default ErrorHandler<T, M> andThen(ErrorHandler<T, M> after) {
			Objects.requireNonNull(after);
			return (T t, M m) -> {
				accept(t, m);
				after.accept(t, m);
			};
		}
	}
}

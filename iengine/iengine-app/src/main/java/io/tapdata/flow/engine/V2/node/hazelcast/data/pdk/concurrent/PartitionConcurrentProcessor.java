package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent;

import com.google.common.collect.Queues;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.PartitionResult;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.Partitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.PartitionKeySelector;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author jackin
 * @date 2022/7/25 16:42
 **/
public class PartitionConcurrentProcessor {

	private final String CONCURRENT_PROCESS_THREAD_NAME_PREFIX = "concurrent-process-thread-";

	private final static String LOG_PREFIX = "[partition concurrent] ";

	private final static int DEFAULT_PARTITION = 0;

	private Logger logger = LogManager.getLogger(PartitionConcurrentProcessor.class);

	protected final ExecutorService executorService;

	protected final List<LinkedBlockingQueue<PartitionEvent<TapdataEvent>>> partitionsQueue;

	private int partitionSize;
	private int batchSize;

	private AtomicBoolean running = new AtomicBoolean(false);

	private Consumer<List<TapdataEvent>> eventProcessor;

	private Partitioner<TapdataEvent, List<Object>> partitioner;

	private PartitionKeySelector<String, TapEvent, Object> keySelector;

	private AtomicLong eventSeq = new AtomicLong(0L);

	private LinkedBlockingQueue<WatermarkEvent> watermarkQueue;

	private Consumer<TapdataEvent> flushOffset;

	public PartitionConcurrentProcessor(
			int partitionSize,
			int batchSize,
			Partitioner<TapdataEvent, List<Object>> partitioner,
			PartitionKeySelector<String, TapEvent, Object> keySelector,
			Consumer<List<TapdataEvent>> eventProcessor,
			Consumer<TapdataEvent> flushOffset
	) {

		this.batchSize = batchSize;

		this.partitionSize = partitionSize;

		this.executorService = new ThreadPoolExecutor(partitionSize + 1, partitionSize + 1,
				60L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(1)
		);

		this.partitionsQueue = IntStream
				.range(0, partitionSize)
				.mapToObj(
						i -> new LinkedBlockingQueue<PartitionEvent<TapdataEvent>>(batchSize * 2)
				).collect(Collectors.toList());

		watermarkQueue = new LinkedBlockingQueue<>(batchSize);

		this.eventProcessor = eventProcessor;

		running.compareAndSet(false, true);

		if (partitioner == null) {
			throw new RuntimeException(LOG_PREFIX + "partitioner cannot be null.");
		}
		this.partitioner = partitioner;

		if (keySelector == null) {
			throw new RuntimeException(LOG_PREFIX + "key selector cannot be null.");
		}
		this.keySelector = keySelector;
		this.flushOffset = flushOffset;
		this.executorService.submit(() -> {
			while (running.get()) {
				try {
					final WatermarkEvent watermarkEvent = watermarkQueue.poll(3, TimeUnit.SECONDS);
					if (watermarkEvent != null) {
						final CountDownLatch countDownLatch = watermarkEvent.getCountDownLatch();
						final TapdataEvent event = watermarkEvent.getEvent();
						while (running.get() && !countDownLatch.await(3, TimeUnit.SECONDS)) {
							if (logger.isInfoEnabled()) {
								final Long sourceTime = event.getSourceTime();
								logger.info("waiting watermark event for all thread process, ts {}", sourceTime != null ? new Date(sourceTime) : null);
							}
						}
						this.flushOffset.accept(event);
					}
				} catch (InterruptedException e) {
					break;
				}
			}
		});
	}

	public void start() {
		for (int partition = 0; partition < partitionSize; partition++) {
			final LinkedBlockingQueue<PartitionEvent<TapdataEvent>> linkedBlockingQueue = partitionsQueue.get(partition);
			int finalPartition = partition;
			executorService.submit(() -> {
				Thread.currentThread().setName(CONCURRENT_PROCESS_THREAD_NAME_PREFIX + finalPartition);
				List<TapdataEvent> processEvents = new ArrayList<>();
				while (running.get()) {
					try {
						List<PartitionEvent<TapdataEvent>> events = new ArrayList<>();
						Queues.drain(linkedBlockingQueue, events, batchSize, 3, TimeUnit.SECONDS);
						if (CollectionUtils.isNotEmpty(events)) {
							for (PartitionEvent partitionEvent : events) {
								if (partitionEvent instanceof NormalEvent) {
									final NormalEvent<?> normalEvent = (NormalEvent<?>) partitionEvent;
									final TapdataEvent event = (TapdataEvent) normalEvent.getEvent();
									processEvents.add(event);
								} else if (partitionEvent instanceof WatermarkEvent) {
									final CountDownLatch countDownLatch = ((WatermarkEvent) partitionEvent).getCountDownLatch();
									countDownLatch.countDown();
								} else {
									if (CollectionUtils.isNotEmpty(processEvents)) {
										eventProcessor.accept(processEvents);
										processEvents.clear();
									}
									final CountDownLatch countDownLatch = ((BarrierEvent) partitionEvent).getCountDownLatch();
									countDownLatch.countDown();
									while (running.get() && !countDownLatch.await(3L, TimeUnit.SECONDS)) {
										if (logger.isDebugEnabled()) {
											logger.debug(LOG_PREFIX + "thread-{} process completed, waiting other thread completed.", finalPartition);
										}
									}
								}
							}
							if (CollectionUtils.isNotEmpty(processEvents)) {
								eventProcessor.accept(processEvents);
								processEvents.clear();
							}
						}
					} catch (InterruptedException e) {
						break;
					}
				}
			});
		}
	}

	public void process(List<TapdataEvent> tapdataEvents, boolean async) {
		if (CollectionUtils.isNotEmpty(tapdataEvents)) {
			for (TapdataEvent tapdataEvent : tapdataEvents) {
				if (tapdataEvent.isDML()) {
					final List<Object> partitionValue = keySelector.select(tapdataEvent.getTapEvent());
					final PartitionResult<TapdataEvent> partitionResult = partitioner.partition(partitionSize, tapdataEvent, partitionValue);
					final int partition = partitionResult.getPartition();
 					final LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue = partitionsQueue.get(partition);
					final NormalEvent<TapdataEvent> normalEvent = new NormalEvent<>(eventSeq.incrementAndGet(), tapdataEvent);
					if (!enqueuePartitionEvent(partition, queue, normalEvent)) {
						break;
					}
				} else {
					generateBarrierEvent();
					final NormalEvent<TapdataEvent> normalEvent = new NormalEvent<>(eventSeq.incrementAndGet(), tapdataEvent);
					if (!enqueuePartitionEvent(DEFAULT_PARTITION, partitionsQueue.get(DEFAULT_PARTITION), normalEvent)) {
						break;
					}
				}
			}
			generateWatermarkEvent(tapdataEvents.get(tapdataEvents.size() - 1));

			if (!async) {
				final BarrierEvent barrierEvent = generateBarrierEvent();
				final CountDownLatch countDownLatch = barrierEvent.getCountDownLatch();
				try {
					while (running.get() && !countDownLatch.await(3, TimeUnit.SECONDS)) {
						if (logger.isInfoEnabled()) {
							logger.info(LOG_PREFIX + "waiting all events processed for thread");
						}
					}
				} catch (InterruptedException e) {
					// nothing to do
				}
			}
		}
	}

	private boolean enqueuePartitionEvent(int partition, LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue, NormalEvent<TapdataEvent> normalEvent) {
		try {
			while (running.get() && !queue.offer(normalEvent, 3, TimeUnit.SECONDS)) {
				if (logger.isInfoEnabled()) {
					logger.info(LOG_PREFIX + "thread-{} process queue if full, waiting for enqueue.", partition);
				}
			}
		} catch (InterruptedException e) {
			// nothing to do
			return false;
		}
		return true;
	}

	private void generateWatermarkEvent(TapdataEvent tapdataEvent) {
		if (CollectionUtils.isNotEmpty(partitionsQueue)) {
			final WatermarkEvent watermarkEvent = new WatermarkEvent(partitionSize, tapdataEvent);
			for (int i = 0; i < partitionsQueue.size(); i++) {
				final LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue = partitionsQueue.get(i);
				try {
					while (running.get() && !queue.offer(watermarkEvent, 3, TimeUnit.SECONDS)) {
						if (logger.isInfoEnabled()) {
							logger.info(LOG_PREFIX + "thread {} queue is full when generate barrier event to queue.", i);
						}
					}
				} catch (InterruptedException e) {
					// nothing to do
					break;
				}
			}

			try {
				while (running.get() && !watermarkQueue.offer(watermarkEvent, 3, TimeUnit.SECONDS)) {
					if (logger.isInfoEnabled()) {
						logger.info(LOG_PREFIX + "watermark queue is full when generate watermark event to queue.");
					}
				}
			} catch (InterruptedException e) {
				// nothing to do
			}
		}
	}

	private BarrierEvent generateBarrierEvent(){
		if (CollectionUtils.isNotEmpty(partitionsQueue)) {
			final BarrierEvent barrierEvent = new BarrierEvent(partitionSize);
			for (int i = 0; i < partitionsQueue.size(); i++) {
				final LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue = partitionsQueue.get(i);
				try {
					while (running.get() && !queue.offer(barrierEvent, 3, TimeUnit.SECONDS)){
						if (logger.isInfoEnabled()) {
							logger.info(LOG_PREFIX + "thread {} queue is full when generate barrier event to queue.", i);
						}
					}
				} catch (InterruptedException e) {
					// nothing to do
					break;
				}
			}

			return barrierEvent;
		}

		return null;
	}

	public void stop(){
		running.compareAndSet(true, false);
		ExecutorUtil.shutdownEx(this.executorService, 60L, TimeUnit.SECONDS);
	}
}

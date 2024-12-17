package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.observable.metric.entity.TaskInputOutputRecordCounter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * @author samuel
 * @Description
 * @create 2024-09-11 17:25
 **/
public class TaskSampleHandlerV2 extends TaskSampleHandler {
	private static final String TAG = TaskSampleHandlerV2.class.getSimpleName();
	private final LinkedBlockingQueue<TaskInputOutputRecordCounter> sourceTaskInputOutputCounterQueue = new LinkedBlockingQueue<>(10000);
	private final LinkedBlockingQueue<TaskInputOutputRecordCounter> targetTaskInputOutputCounterQueue = new LinkedBlockingQueue<>(10000);
	private TaskInputOutputRecordCounter tempInputOutputCounter;
	private Thread consumeInputOutputThread;
	private int missingTarget = 0;
	private final AtomicBoolean isConsumeInputOutputThreadRunning = new AtomicBoolean(false);

	public TaskSampleHandlerV2(TaskDto task) {
		super(task);
	}

	@Override
	public void doInit(Map<String, Number> values) {
		super.doInit(values);
		if (isConsumeInputOutputThreadRunning.compareAndSet(false, true)) {
			this.consumeInputOutputThread = new Thread(() -> {
				while (!Thread.currentThread().isInterrupted()) {
					try {
						consumeQueue(TimeUnit.SECONDS.toMillis(5L));
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
				}
				while (!this.sourceTaskInputOutputCounterQueue.isEmpty()) {
					TaskInputOutputRecordCounter sourceCounter = this.sourceTaskInputOutputCounterQueue.poll();
					TaskInputOutputRecordCounter targetCounter = this.targetTaskInputOutputCounterQueue.poll();
					if (null == sourceCounter || null == targetCounter) {
						break;
					}
					incInput(sourceCounter);
					incOutput(targetCounter);
				}
				Thread.currentThread().interrupt();
			});
			this.consumeInputOutputThread.setName(String.join("-", TAG, "consume", "input", "output", "counter", "thread", task.getName(), task.getId().toString()));
			this.consumeInputOutputThread.start();
		}
	}

	@Override
	public void close() {
		CommonUtils.ignoreAnyError(() -> {
			Optional.ofNullable(consumeInputOutputThread).ifPresent(Thread::interrupt);
			this.isConsumeInputOutputThreadRunning.set(false);
		}, TAG);
		super.close();
	}

	protected void incOutput(TaskInputOutputRecordCounter targetCounter) {
		outputInsertCounter.inc(targetCounter.getInsertCounter().value().longValue());
		outputUpdateCounter.inc(targetCounter.getUpdateCounter().value().longValue());
		outputDeleteCounter.inc(targetCounter.getDeleteCounter().value().longValue());
	}

	protected void incInput(TaskInputOutputRecordCounter sourceCounter) {
		inputInsertCounter.inc(sourceCounter.getInsertCounter().value().longValue());
		inputUpdateCounter.inc(sourceCounter.getUpdateCounter().value().longValue());
		inputDeleteCounter.inc(sourceCounter.getDeleteCounter().value().longValue());
	}

	@Override
	public void handleBatchReadAccept(HandlerUtil.EventTypeRecorder recorder) {
		if (null == recorder) {
			return;
		}
		long size = recorder.getInsertTotal();
		inputSizeSpeed.add(recorder.getMemorySize());
		TaskInputOutputRecordCounter taskInputOutputRecordCounter = new TaskInputOutputRecordCounter();
		taskInputOutputRecordCounter.getInsertCounter().inc(size);
		try {
			sourceTaskInputOutputCounterQueue.put(taskInputOutputRecordCounter);
		} catch (InterruptedException ignored) {
			Thread.currentThread().interrupt();
		}
		inputSpeed.add(size);
	}

	@Override
	public void handleStreamReadAccept(HandlerUtil.EventTypeRecorder recorder) {
		if (null == recorder) {
			return;
		}
		TaskInputOutputRecordCounter taskInputOutputRecordCounter = new TaskInputOutputRecordCounter();
		taskInputOutputRecordCounter.getInsertCounter().inc(recorder.getInsertTotal());
		taskInputOutputRecordCounter.getUpdateCounter().inc(recorder.getUpdateTotal());
		taskInputOutputRecordCounter.getDeleteCounter().inc(recorder.getDeleteTotal());
		try {
			sourceTaskInputOutputCounterQueue.put(taskInputOutputRecordCounter);
		} catch (InterruptedException ignored) {
			Thread.currentThread().interrupt();
		}
		inputDdlCounter.inc(recorder.getDdlTotal());
		inputOthersCounter.inc(recorder.getOthersTotal());
		inputSizeSpeed.add(recorder.getMemorySize());
		inputSpeed.add(recorder.getTotal());
	}

	@Override
	public void handleWriteRecordAccept(WriteListResult<TapRecordEvent> result, List<TapRecordEvent> events, HandlerUtil.EventTypeRecorder eventTypeRecorder) {
		long current = System.currentTimeMillis();
		long inserted = 0L;
		long updated = 0L;
		long deleted = 0L;
		long total = 0L;
		if (null != result) {
			inserted = result.getInsertedCount();
			updated = result.getModifiedCount();
			deleted = result.getRemovedCount();
			total = inserted + updated + deleted;
		}

		if (null == tempInputOutputCounter) {
			tempInputOutputCounter = new TaskInputOutputRecordCounter();
		}
		tempInputOutputCounter.getInsertCounter().inc(inserted);
		tempInputOutputCounter.getUpdateCounter().inc(updated);
		tempInputOutputCounter.getDeleteCounter().inc(deleted);

		outputSpeed.add(total);

		snapshotInsertRowTotal.inc(total);
		if (Objects.isNull(currentSnapshotTableInsertRowTotal)) {
			currentSnapshotTableInsertRowTotal = total;
		} else {
			currentSnapshotTableInsertRowTotal += total;
		}

		long timeCostTotal = 0L;
		if (null != events) {
			for (TapRecordEvent event : events) {
				Long time = event.getTime();
				if (null == time) {
					TapLogger.warn(TAG, "event from task {} does have time field.", task.getId().toHexString());
					break;
				}
				timeCostTotal += (current - time);
			}
		}
		timeCostAverage.add(total, timeCostTotal);
		if (null != eventTypeRecorder) {
			outputSizeSpeed.add(eventTypeRecorder.getMemorySize());
		}
	}

	public void handleWriteBatchSplit() {
		TaskInputOutputRecordCounter taskInputOutputRecordCounter = null != tempInputOutputCounter ? tempInputOutputCounter : new TaskInputOutputRecordCounter();
		try {
			targetTaskInputOutputCounterQueue.put(taskInputOutputRecordCounter);
		} catch (InterruptedException ignored) {
			Thread.currentThread().interrupt();
			// do nothing
		}
		this.tempInputOutputCounter = null;
	}

	protected void consumeQueue(long targetQueuePollTimeoutMs) throws InterruptedException {
		TaskInputOutputRecordCounter sourceCounter = sourceTaskInputOutputCounterQueue.take();
		List<TaskInputOutputRecordCounter> targetCounters = new ArrayList<>();
		if (missingTarget > 0) {
			int loopTime = missingTarget;
			IntStream.range(0, loopTime).forEach(i -> {
				TaskInputOutputRecordCounter targetCounter = targetTaskInputOutputCounterQueue.poll();
				if (null != targetCounter) {
					targetCounters.add(targetCounter);
					missingTarget--;
				}
			});
		}
		TaskInputOutputRecordCounter targetCounter;
		if (targetQueuePollTimeoutMs > 0) {
			targetCounter = targetTaskInputOutputCounterQueue.poll(targetQueuePollTimeoutMs, TimeUnit.MILLISECONDS);
		} else {
			targetCounter = targetTaskInputOutputCounterQueue.poll();
		}
		if (null != targetCounter) {
			targetCounters.add(targetCounter);
		} else {
			missingTarget++;
		}
		incInput(sourceCounter);
		for (TaskInputOutputRecordCounter counter : targetCounters) {
			incOutput(counter);
		}
	}
}

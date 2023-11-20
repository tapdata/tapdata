package io.tapdata.aspect;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

public class ProcessorNodeProcessAspect extends ProcessorFunctionAspect<ProcessorNodeProcessAspect> {
	private static final String TAG = ProcessorNodeProcessAspect.class.getSimpleName();
	private final LongAdder counter = new LongAdder();
	private List<Consumer<TapdataEvent>> consumers = new CopyOnWriteArrayList<>();
	public static final int STATE_PROCESSING = 10;
	public ProcessorNodeProcessAspect consumer(Consumer<TapdataEvent> listConsumer) {
		this.consumers.add(tapdataEvent -> {
			try {
				listConsumer.accept(tapdataEvent);
				counter.increment();
			} catch(Exception e) {
				TapLogger.warn(TAG, "Consume outputEvent {} for inputEvent {} failed on consumer {}, {}", tapdataEvent, inputEvent, listConsumer, Log4jUtil.getStackString(e));
			}
		});
		return this;
	}

	public long outputCount() {
		return counter.longValue();
	}
	private TapdataEvent inputEvent;
	private List<TapdataEvent> inputEvents;
	public ProcessorNodeProcessAspect inputEvent(TapdataEvent inputEvent) {
		this.inputEvent = inputEvent;
		return this;
	}

	public ProcessorNodeProcessAspect inputEvents(List<TapdataEvent> inputEvents) {
		this.inputEvents = inputEvents;
		return this;
	}

	public LongAdder getCounter() {
		return counter;
	}

	public TapdataEvent getInputEvent() {
		return inputEvent;
	}

	public void setInputEvent(TapdataEvent inputEvent) {
		this.inputEvent = inputEvent;
	}

	public List<Consumer<TapdataEvent>> getConsumers() {
		return consumers;
	}

	public void setConsumers(List<Consumer<TapdataEvent>> consumers) {
		this.consumers = consumers;
	}

	public List<TapdataEvent> getInputEvents() {
		if (CollectionUtils.isNotEmpty(inputEvents)) {
			return inputEvents;
		} else {
			return Collections.singletonList(inputEvent);
		}
	}

	public void setInputEvents(List<TapdataEvent> inputEvents) {
		this.inputEvents = inputEvents;
	}
}

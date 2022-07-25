package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

public class ProcessorNodeProcessAspect extends ProcessorFunctionAspect<ProcessorNodeProcessAspect> {
	private static final String TAG = ProcessorNodeProcessAspect.class.getSimpleName();
	private final LongAdder counter = new LongAdder();
	private Consumer<TapdataEvent> consumer;
	public ProcessorNodeProcessAspect consumer(Consumer<TapdataEvent> listConsumer) {
		this.consumer = tapdataEvent -> {
			try {
				listConsumer.accept(tapdataEvent);
				counter.increment();
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume outputEvent {} for inputEvent {} failed on consumer {}, {}", tapdataEvent, inputEvent, listConsumer, ExceptionUtils.getStackTrace(throwable));
			}
		};
		return this;
	}

	public long outputCount() {
		return counter.longValue();
	}
	private TapdataEvent inputEvent;
	public ProcessorNodeProcessAspect inputEvent(TapdataEvent inputEvent) {
		this.inputEvent = inputEvent;
		return this;
	}
	private Long outputTime;
	public ProcessorNodeProcessAspect outputTime(Long outputTime) {
		this.outputTime = outputTime;
		return this;
	}

	public static final int STATE_OUTPUT = 10;

	public LongAdder getCounter() {
		return counter;
	}

	public TapdataEvent getInputEvent() {
		return inputEvent;
	}

	public void setInputEvent(TapdataEvent inputEvent) {
		this.inputEvent = inputEvent;
	}

	public Long getOutputTime() {
		return outputTime;
	}

	public void setOutputTime(Long outputTime) {
		this.outputTime = outputTime;
	}

	public Consumer<TapdataEvent> getConsumer() {
		return consumer;
	}

	public void setConsumer(Consumer<TapdataEvent> consumer) {
		this.consumer = consumer;
	}
}

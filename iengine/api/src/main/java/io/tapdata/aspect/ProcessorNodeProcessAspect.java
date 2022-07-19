package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;

import java.util.concurrent.atomic.LongAdder;

public class ProcessorNodeProcessAspect extends ProcessorFunctionAspect<ProcessorNodeProcessAspect> {
	private final LongAdder counter = new LongAdder();
	private TapdataEvent outputEvent;
	public ProcessorNodeProcessAspect outputEvent(TapdataEvent outputEvent) {
		this.outputEvent = outputEvent;
		counter.increment();
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

	public TapdataEvent getOutputEvent() {
		return outputEvent;
	}

	public void setOutputEvent(TapdataEvent outputEvent) {
		this.outputEvent = outputEvent;
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
}

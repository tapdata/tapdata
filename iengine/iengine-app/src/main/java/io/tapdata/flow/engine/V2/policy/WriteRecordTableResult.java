package io.tapdata.flow.engine.V2.policy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author samuel
 * @Description
 * @create 2024-07-09 15:37
 **/
public class WriteRecordTableResult {
	private final AtomicInteger duplicateKeyErrorCounter = new AtomicInteger();
	private final AtomicBoolean continuousDuplicateKeyErrorOverLimit = new AtomicBoolean();

	public int incrementDuplicateKeyErrorCounter() {
		return duplicateKeyErrorCounter.getAndIncrement();
	}

	public int getDuplicateKeyErrorCounter() {
		return duplicateKeyErrorCounter.get();
	}

	public void resetDuplicateKeyErrorCounter() {
		duplicateKeyErrorCounter.set(0);
	}

	public AtomicBoolean getContinuousDuplicateKeyErrorOverLimit() {
		return continuousDuplicateKeyErrorOverLimit;
	}
}

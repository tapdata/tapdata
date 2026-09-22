package com.tapdata.entity;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

/**
 * @author samuel
 * @Description
 * @create 2024-08-12 11:46
 **/
public class TapdataCountDownLatchEvent extends TapdataEvent implements Serializable {
	// Keep the implicit serialVersionUID of the pre-DLQ class so an Engine
	// rolling upgrade can still deserialize an already queued barrier event.
	private static final long serialVersionUID = 4600489008835618970L;

	private final int initCount;
	private transient CountDownLatch countDownLatch;
	private String dqlRecoveryBarrierId;

	public TapdataCountDownLatchEvent(int initCount) {
		this.initCount = initCount;
		this.countDownLatch = new CountDownLatch(initCount);
	}

	public static TapdataCountDownLatchEvent create(int initCount) {
		return new TapdataCountDownLatchEvent(initCount);
	}

	public int getInitCount() {
		return initCount;
	}

	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}

	public String getDqlRecoveryBarrierId() {
		return dqlRecoveryBarrierId;
	}

	public void setDqlRecoveryBarrierId(String dqlRecoveryBarrierId) {
		this.dqlRecoveryBarrierId = dqlRecoveryBarrierId;
	}

	private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
		inputStream.defaultReadObject();
		this.countDownLatch = new CountDownLatch(initCount);
	}

	@Override
	public boolean isConcurrentWrite() {
		return false;
	}
}

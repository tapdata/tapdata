package com.tapdata.entity;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

/**
 * @author samuel
 * @Description
 * @create 2024-08-12 11:46
 **/
public class TapdataCountDownLatchEvent extends TapdataEvent implements Serializable {

	private final int initCount;
	private final CountDownLatch countDownLatch;

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

	@Override
	public boolean isConcurrentWrite() {
		return false;
	}
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent;

import java.util.concurrent.CountDownLatch;

/**
 * @author jackin
 * @date 2022/7/25 16:39
 **/
public class BarrierEvent implements PartitionEvent {

	private CountDownLatch countDownLatch;

	public BarrierEvent(int count) {
		this.countDownLatch = new CountDownLatch(count);
	}

	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}
}

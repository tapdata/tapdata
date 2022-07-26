package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent;

import com.tapdata.entity.TapdataEvent;

import java.util.concurrent.CountDownLatch;

/**
 * @author jackin
 * @date 2022/7/26 21:42
 **/
public class WatermarkEvent implements PartitionEvent<TapdataEvent> {

	private CountDownLatch countDownLatch;

	private TapdataEvent event;

	public WatermarkEvent(int partitionSize, TapdataEvent event) {
		this.countDownLatch = new CountDownLatch(partitionSize);
		this.event = event;
	}

	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}

	public TapdataEvent getEvent() {
		return event;
	}
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent;

/**
 * @author jackin
 * @date 2022/7/25 16:39
 **/
public class NormalEvent<T> implements PartitionEvent<T> {

	private long eventSeqNo;

	private T event;

	public NormalEvent(long eventSeqNo, T tapdataEvent) {
		this.eventSeqNo = eventSeqNo;
		this.event = tapdataEvent;
	}

	public long getEventSeqNo() {
		return eventSeqNo;
	}

	public void setEventSeqNo(long eventSeqNo) {
		this.eventSeqNo = eventSeqNo;
	}

	public T getEvent() {
		return event;
	}

	public void setEvent(T event) {
		this.event = event;
	}
}

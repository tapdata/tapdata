package io.tapdata.entity.event.dml;


import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;

import java.util.Map;

public class TapInsertRecordEvent extends TapRecordEvent {
	public static final int TYPE = 300;
	/**
	 * The latest record, after insert and update
	 * Value format should follow TapType formats
	 */
	private Map<String, Object> after;

	public TapInsertRecordEvent() {
		super(TYPE);
	}

	public void clone(TapEvent tapEvent) {
		super.clone(tapEvent);
		if (tapEvent instanceof TapInsertRecordEvent) {
			TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) tapEvent;
			if (after != null)
				insertRecordEvent.after = InstanceFactory.instance(TapUtils.class).cloneMap(after);
		}
	}

	public TapInsertRecordEvent init() {
		time = System.currentTimeMillis();
		return this;
	}

	public TapInsertRecordEvent referenceTime(Long referenceTime) {
		this.referenceTime = referenceTime;
		return this;
	}

	public TapInsertRecordEvent after(Map<String, Object> after) {
		this.after = after;
		return this;
	}

	public TapInsertRecordEvent table(String table) {
		this.tableId = table;
		return this;
	}

	public Map<String, Object> getAfter() {
		return after;
	}

	public void setAfter(Map<String, Object> after) {
		this.after = after;
	}

	@Override
	public String toString() {
		return "TapInsertRecordEvent{" +
				"after=" + after +
				"} " + super.toString();
	}
}

package io.tapdata.entity.event.dml;


import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;

import java.util.Map;

public class TapDeleteRecordEvent extends TapRecordEvent {
	public static final int TYPE = 301;
	private Map<String, Object> before;

	public TapDeleteRecordEvent() {
		super(TYPE);
	}

	@Override
	public void clone(TapEvent tapEvent) {
		super.clone(tapEvent);
		if (tapEvent instanceof TapDeleteRecordEvent) {
			TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) tapEvent;
			if (before != null)
				deleteRecordEvent.before = InstanceFactory.instance(TapUtils.class).cloneMap(before);
		}
	}

	public TapDeleteRecordEvent init() {
		time = System.currentTimeMillis();
		return this;
	}

	public TapDeleteRecordEvent referenceTime(Long referenceTime) {
		this.referenceTime = referenceTime;
		return this;
	}

	public TapDeleteRecordEvent before(Map<String, Object> before) {
		this.before = before;
		return this;
	}

	public TapDeleteRecordEvent table(String table) {
		this.tableId = table;
		return this;
	}

	public Map<String, Object> getBefore() {
		return before;
	}

	public void setBefore(Map<String, Object> before) {
		this.before = before;
	}

	@Override
	public String toString() {
		return "TapDeleteRecordEvent{" +
				"before=" + before +
				"} " + super.toString();
	}
}

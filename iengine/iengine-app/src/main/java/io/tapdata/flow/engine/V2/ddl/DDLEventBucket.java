package io.tapdata.flow.engine.V2.ddl;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description DDL event buffer
 * @create 2022-07-07 14:58
 **/
public class DDLEventBucket {
	private static final long TIME_OUT_MS = 60 * 1000L;
	private List<TapdataEvent> tapDDLEventList;
	private long lastUpdateTime;

	public static DDLEventBucket create() {
		return new DDLEventBucket();
	}

	public void addEvent(TapdataEvent tapdataEvent) {
		if (null == tapDDLEventList) {
			tapDDLEventList = new ArrayList<>();
		}
		if (!(tapdataEvent.getTapEvent() instanceof TapDDLEvent)) {
			return;
		}
		tapDDLEventList.add(tapdataEvent);
		this.lastUpdateTime = System.currentTimeMillis();
	}

	public boolean isTimeOut() {
		if (CollectionUtils.isEmpty(tapDDLEventList) || lastUpdateTime <= 0L) {
			return false;
		}
		long interval = System.currentTimeMillis() - lastUpdateTime;
		return interval > TIME_OUT_MS;
	}

	public List<TapdataEvent> getTapDDLEventList() {
		return tapDDLEventList;
	}

	public void setTapDDLEventList(List<TapdataEvent> tapDDLEventList) {
		this.tapDDLEventList = tapDDLEventList;
	}

	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	public void setLastUpdateTime(long lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
	}
}

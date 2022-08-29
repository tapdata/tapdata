package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;

import java.util.ArrayList;
import java.util.List;

public class SourceDynamicTableAspect extends DataNodeAspect<SourceDynamicTableAspect> {

	public static final int DYNAMIC_TABLE_TYPE_ADD = 100;
	public static final int DYNAMIC_TABLE_TYPE_REMOVE = 110;

	private List<TapdataEvent> tapdataEvents;
	public SourceDynamicTableAspect tapdataEvents(List<TapdataEvent> tapdataEvents) {
		this.tapdataEvents = tapdataEvents;
		return this;
	}

	private List<String> tables;
	public SourceDynamicTableAspect tables (List<String> tables) {
		this.tables = tables;
		return this;
	}
	public SourceDynamicTableAspect table (String table) {
		if (null == tables) {
			this.tables = new ArrayList<>();
		}
		this.tables.add(table);
		return this;
	}


	public SourceDynamicTableAspect tapdataEvents(TapdataEvent tapdataEvent) {
		if (this.tapdataEvents == null) {
			this.tapdataEvents = new ArrayList<>();
		}
		this.tapdataEvents.add(tapdataEvent);
		return this;
	}

	private int type;
	public SourceDynamicTableAspect type(int type) {
		this.type = type;
		return this;
	}

	public List<TapdataEvent> getTapdataEvents() {
		return tapdataEvents;
	}

	public void setTapdataEvents(List<TapdataEvent> tapdataEvents) {
		this.tapdataEvents = tapdataEvents;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public List<String> getTables() {
		return tables;
	}

	public void setTables(List<String> tables) {
		this.tables = tables;
	}
}

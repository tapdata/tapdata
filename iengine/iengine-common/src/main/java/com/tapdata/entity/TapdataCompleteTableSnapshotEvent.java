package com.tapdata.entity;

/**
 * @author samuel
 * @Description
 * @create 2023-05-19 17:07
 **/
public class TapdataCompleteTableSnapshotEvent extends TapdataEvent {

	private static final long serialVersionUID = 265422482461825374L;

	private String sourceTableName;

	public TapdataCompleteTableSnapshotEvent(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	public String getSourceTableName() {
		return sourceTableName;
	}

	private void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	@Override
	protected void clone(TapdataEvent tapdataEvent) {
		super.clone(tapdataEvent);
		((TapdataCompleteTableSnapshotEvent) tapdataEvent).setSourceTableName(sourceTableName);
	}
}

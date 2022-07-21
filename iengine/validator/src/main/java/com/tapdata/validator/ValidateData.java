package com.tapdata.validator;

import com.tapdata.entity.InconsistentData;

import java.util.Map;

public class ValidateData {

	private Object event;

	private Map<String, Object> sourceRow;

	private Map<String, Object> targetRow;

	private boolean secondValidate;

	private InconsistentData inconsistentData;

	private String fromTable;

	private String toTable;

	public String getFromTable() {
		return fromTable;
	}

	public void setFromTable(String fromTable) {
		this.fromTable = fromTable;
	}

	public String getToTable() {
		return toTable;
	}

	public void setToTable(String toTable) {
		this.toTable = toTable;
	}

	public Map<String, Object> getSourceRow() {
		return sourceRow;
	}

	public void setSourceRow(Map<String, Object> sourceRow) {
		this.sourceRow = sourceRow;
	}

	public Map<String, Object> getTargetRow() {
		return targetRow;
	}

	public void setTargetRow(Map<String, Object> targetRow) {
		this.targetRow = targetRow;
	}

	public Object getEvent() {
		return event;
	}

	public void setEvent(Object event) {
		this.event = event;
	}

	public boolean isSecondValidate() {
		return secondValidate;
	}

	public void setSecondValidate(boolean secondValidate) {
		this.secondValidate = secondValidate;
	}

	public InconsistentData getInconsistentData() {
		return inconsistentData;
	}

	public void setInconsistentData(InconsistentData inconsistentData) {
		this.inconsistentData = inconsistentData;
	}
}

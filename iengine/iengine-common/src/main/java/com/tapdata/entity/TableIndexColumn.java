package com.tapdata.entity;

import java.io.Serializable;

public class TableIndexColumn implements Serializable, Cloneable {

	private static final long serialVersionUID = 1935889832252700294L;

	private String columnName;

	private int columnPosition;

	private Boolean columnIsAsc;

	private Object columnValue;

	public TableIndexColumn() {
	}

	public TableIndexColumn(String columnName, int columnPosition, Boolean columnIsAsc) {
		this.columnName = columnName;
		this.columnPosition = columnPosition;
		this.columnIsAsc = columnIsAsc;
	}

	public TableIndexColumn(String columnName, int columnPosition, Object columnValue) {
		this.columnName = columnName;
		this.columnPosition = columnPosition;
		this.columnValue = columnValue;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public int getColumnPosition() {
		return columnPosition;
	}

	public void setColumnPosition(int columnPosition) {
		this.columnPosition = columnPosition;
	}

	public Boolean getColumnIsAsc() {
		return columnIsAsc;
	}

	public void setColumnIsAsc(Boolean columnIsAsc) {
		this.columnIsAsc = columnIsAsc;
	}

	public Object getColumnValue() {
		return columnValue;
	}

	public void setColumnValue(Object columnValue) {
		this.columnValue = columnValue;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public String toString() {
		return "TableIndexColumn{" +
				"columnName='" + columnName + '\'' +
				", columnPosition=" + columnPosition +
				", columnIsAsc=" + columnIsAsc +
				'}';
	}
}

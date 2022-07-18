package com.tapdata.entity;

/**
 * Created by tapdata on 28/07/2017.
 */
public class TableConfig {

	private String collection;

	private Object fields;

	private String insertionType;

	private String table;

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public Object getFields() {
		return fields;
	}

	public void setFields(Object fields) {
		this.fields = fields;
	}

	public String getInsertionType() {
		return insertionType;
	}

	public void setInsertionType(String insertionType) {
		this.insertionType = insertionType;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	@Override
	public String toString() {
		return "TableConfig{" +
				"collection='" + collection + '\'' +
				", fields=" + fields +
				", insertionType='" + insertionType + '\'' +
				", table='" + table + '\'' +
				'}';
	}
}

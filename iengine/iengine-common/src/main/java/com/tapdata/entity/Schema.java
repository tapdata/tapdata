package com.tapdata.entity;

import io.tapdata.entity.schema.TapTable;

import java.io.Serializable;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2020-08-21 11:31
 **/
public class Schema implements Serializable {

	private static final long serialVersionUID = 3658306210764896092L;

	private List<RelateDataBaseTable> tables;
	private List<TapTable> tapTables;
	private boolean includeFields = true;
	private int tapTableCount;

	public Schema() {

	}

	public Schema(List<RelateDataBaseTable> tables) {
		this.tables = tables;
	}

	public Schema(List<RelateDataBaseTable> tables, boolean includeFields) {
		this.tables = tables;
		this.includeFields = includeFields;
	}

	public Schema(boolean includeFields, int tapTableCount) {
		this.includeFields = includeFields;
		this.tapTableCount = tapTableCount;
	}

	public List<RelateDataBaseTable> getTables() {
		return tables;
	}

	public void setTables(List<RelateDataBaseTable> tables) {
		this.tables = tables;
	}

	public boolean isIncludeFields() {
		return includeFields;
	}

	public void setIncludeFields(boolean includeFields) {
		this.includeFields = includeFields;
	}

	public List<TapTable> getTapTables() {
		return tapTables;
	}

	public void setTapTables(List<TapTable> tapTables) {
		this.tapTables = tapTables;
	}

	public int getTapTableCount() {
		return tapTableCount;
	}

	public void setTapTableCount(int tapTableCount) {
		this.tapTableCount = tapTableCount;
	}
}

package com.tapdata.constant;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.util.CellRangeAddress;

import java.util.List;

public class ExcelHeader {

	private List<String> fieldNames;
	private List<Cell> cells;
	private boolean isArray;
	private boolean isNullHeader;
	private CellRangeAddress lastMergedRangion;
	private String fieldname;

	public ExcelHeader() {
		isNullHeader = false;
	}

	public List<String> getFieldNames() {
		return fieldNames;
	}

	public void setFieldNames(List<String> fieldNames) {
		this.fieldNames = fieldNames;
	}

	public boolean isArray() {
		return isArray;
	}

	public void setArray(boolean array) {
		isArray = array;
	}

	public List<Cell> getCells() {
		return cells;
	}

	public void setCells(List<Cell> cells) {
		this.cells = cells;
	}

	public boolean isNullHeader() {
		return isNullHeader;
	}

	public void setNullHeader(boolean nullHeader) {
		isNullHeader = nullHeader;
	}

	public CellRangeAddress getLastMergedRangion() {
		return lastMergedRangion;
	}

	public void setLastMergedRangion(CellRangeAddress lastMergedRangion) {
		this.lastMergedRangion = lastMergedRangion;
	}

	public String getFieldname() {
		return fieldname;
	}

	public void setFieldname(String fieldname) {
		this.fieldname = fieldname;
	}

	@Override
	public String toString() {
		return "ExcelHeader{" +
				"fieldNames=" + fieldNames +
				", cells=" + cells +
				", isArray=" + isArray +
				", isNullHeader=" + isNullHeader +
				", lastMergedRangion=" + lastMergedRangion +
				", fieldname='" + fieldname + '\'' +
				'}';
	}
}

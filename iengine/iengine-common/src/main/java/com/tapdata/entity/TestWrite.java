package com.tapdata.entity;

import java.io.Serializable;

public class TestWrite implements Serializable {

	public final static String COL_VALUE = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
	public final static String TABLE_NAME = "testWrite";
	public final static String PK_FEILD_NAME = "id";
	private static final long serialVersionUID = 4717354425607330319L;

	private long rows;

	private int col_length;

	private boolean is_bulk_result;

	public long getRows() {
		return rows;
	}

	public void setRows(long rows) {
		this.rows = rows;
	}

	public int getCol_length() {
		return col_length;
	}

	public void setCol_length(int col_length) {
		this.col_length = col_length;
	}

	public boolean is_bulk_result() {
		return is_bulk_result;
	}

	public void setIs_bulk_result(boolean is_bulk_result) {
		this.is_bulk_result = is_bulk_result;
	}
}

package com.tapdata.entity;

import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcColumns {
	private final static String COLUMN_NAME = "COLUMN_NAME";
	private final static String TYPE_NAME = "TYPE_NAME";
	private final static String COLUMN_SIZE = "COLUMN_SIZE";
	private final static String DECIMAL_DIGITS = "DECIMAL_DIGITS";
	private final static String NULLABLE = "NULLABLE";

	private String columnName;
	private String typeName;
	private int columnSize;
	private int decimalDigits;
	private int nullable;

	public JdbcColumns(ResultSet columns) throws SQLException {
		if (columns != null) {
			columnName = columns.getString(COLUMN_NAME);
			typeName = columns.getString(TYPE_NAME);
			columnSize = columns.getInt(COLUMN_SIZE);
			decimalDigits = columns.getInt(DECIMAL_DIGITS);
			nullable = columns.getInt(NULLABLE);
		}
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public int getColumnSize() {
		return columnSize;
	}

	public void setColumnSize(int columnSize) {
		this.columnSize = columnSize;
	}

	public int getDecimalDigits() {
		return decimalDigits;
	}

	public void setDecimalDigits(int decimalDigits) {
		this.decimalDigits = decimalDigits;
	}

	public int getNullable() {
		return nullable;
	}

	public void setNullable(int nullable) {
		this.nullable = nullable;
	}
}

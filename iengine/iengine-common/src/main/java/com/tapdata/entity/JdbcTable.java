package com.tapdata.entity;

import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcTable {

	private final static String TABLE_CAT = "TABLE_CAT";
	private final static String TABLE_SCHEM = "TABLE_SCHEM";
	private final static String TABLE_NAME = "TABLE_NAME";
	private final static String TABLE_TYPE = "TABLE_TYPE";
	private final static String REMARKS = "REMARKS";

	private String table_cat;
	private String table_schem;
	private String table_name;
	private String table_type;
	private String remarks;

	public JdbcTable(String table_cat, String table_schem, String table_name, String type_type, String remarks) {
		this.table_cat = table_cat;
		this.table_schem = table_schem;
		this.table_name = table_name;
		this.table_type = type_type;
		this.remarks = remarks;
	}

	public static JdbcTable getJdbcTable(ResultSet rs) {
		if (rs != null) {
			try {
				return new JdbcTable(rs.getString(TABLE_CAT), rs.getString(TABLE_SCHEM), rs.getString(TABLE_NAME), rs.getString(TABLE_TYPE), rs.getString(REMARKS));
			} catch (SQLException e) {
				return null;
			}
		} else {
			return null;
		}
	}

	public String getTable_cat() {
		return table_cat;
	}

	public String getTable_schem() {
		return table_schem;
	}

	public String getTable_name() {
		return table_name;
	}

	public String getTable_type() {
		return table_type;
	}

	public String getRemarks() {
		return remarks;
	}
}

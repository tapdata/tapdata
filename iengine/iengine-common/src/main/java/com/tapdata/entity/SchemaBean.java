package com.tapdata.entity;

public class SchemaBean {

	private String pk_column_name;

	private String pk_table_name;

	public SchemaBean() {
	}

	public SchemaBean(String pk_column_name, String pk_table_name) {
		this.pk_column_name = pk_column_name;
		this.pk_table_name = pk_table_name;
	}

	public String getPk_column_name() {
		return pk_column_name;
	}

	public void setPk_column_name(String pk_column_name) {
		this.pk_column_name = pk_column_name;
	}

	public String getPk_table_name() {
		return pk_table_name;
	}

	public void setPk_table_name(String pk_table_name) {
		this.pk_table_name = pk_table_name;
	}

}

package io.tapdata.entity;

import org.apache.commons.lang3.StringUtils;

public class Converter {

	private String databaseType;

	private Class<?> clazz;

	public Converter() {

	}

	public Converter(String databaseType, Class<?> clazz) {
		this.databaseType = databaseType;
		this.clazz = clazz;
	}

	public String getDatabaseType() {
		return databaseType;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

	public Class<?> getClazz() {
		return clazz;
	}

	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}

	public boolean isEmpty() {
		return StringUtils.isBlank(databaseType)
				|| clazz == null;
	}
}

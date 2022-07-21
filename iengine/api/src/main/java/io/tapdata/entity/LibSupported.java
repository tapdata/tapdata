package io.tapdata.entity;

import java.util.Map;

public class LibSupported {

	private String databaseType;

	private Map<String, Boolean> supportedList;

	public LibSupported(String databaseType, Map<String, Boolean> supportedList) {
		this.databaseType = databaseType;
		this.supportedList = supportedList;
	}

	public String getDatabaseType() {
		return databaseType;
	}

	public Map<String, Boolean> getSupportedList() {
		return supportedList;
	}
}

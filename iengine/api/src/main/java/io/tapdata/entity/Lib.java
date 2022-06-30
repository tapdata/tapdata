package io.tapdata.entity;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class Lib {

	private String databaseType;

	private Class<?> clazz;

	private Boolean isSource = false;

	private Boolean isTarget = false;

	private List<String> databaseTypes;

	public String getDatabaseType() {
		return databaseType;
	}

	public Lib setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
		return this;
	}

	public Class<?> getClazz() {
		return clazz;
	}

	public Lib setClazz(Class<?> clazz) {
		this.clazz = clazz;
		return this;
	}

	public Boolean getSource() {
		return isSource;
	}

	public Lib setSource(Boolean source) {
		isSource = source;
		return this;
	}

	public Boolean getTarget() {
		return isTarget;
	}

	public Lib setTarget(Boolean target) {
		isTarget = target;
		return this;
	}

	public List<String> getDatabaseTypes() {
		return databaseTypes;
	}

	public void setDatabaseTypes(List<String> databaseTypes) {
		this.databaseTypes = databaseTypes;
	}

	@Override
	public String toString() {
		return "Lib{" +
				"clazz=" + clazz +
				", isSource=" + isSource +
				", isTarget=" + isTarget +
				", databaseTypes=" + databaseTypes +
				'}';
	}

	public boolean isEmpty() {
		return clazz == null
				|| CollectionUtils.isEmpty(databaseTypes);
	}
}

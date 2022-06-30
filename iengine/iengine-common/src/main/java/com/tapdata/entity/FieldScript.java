package com.tapdata.entity;

import java.io.Serializable;

/**
 * @author jackin
 */
public class FieldScript implements Serializable {

	private static final long serialVersionUID = -4052784074794004988L;
	private String field;

	private String tableName;

	private String scriptType;

	private String script;

	public FieldScript() {
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getScriptType() {
		return scriptType;
	}

	public void setScriptType(String scriptType) {
		this.scriptType = scriptType;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}
}

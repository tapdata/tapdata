/**
 * @title: FieldScript
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class FieldScript {

	private String field;

	private String tableName;

	private String scriptType;

	private String script;

	public String getField() {
		return field;
	}

	public String getTableName() {
		return tableName;
	}

	public String getScriptType() {
		return scriptType;
	}

	public String getScript() {
		return script;
	}

	public void setField(String field) {
		this.field = field;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setScriptType(String scriptType) {
		this.scriptType = scriptType;
	}

	public void setScript(String script) {
		this.script = script;
	}
}

package io.tapdata.connector.mysql.ddl.type;

import io.tapdata.connector.mysql.ddl.DDLWrapper;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 17:17
 **/
public class DDLType {
	private Type type;
	private String pattern;
	private boolean caseSensitive;
	private String desc;
	private Class<? extends DDLWrapper<?>>[] ddlWrappers;

	public DDLType(Type type, String pattern, boolean caseSensitive, String desc, Class<? extends DDLWrapper<?>>... ddlWrappers) {
		this.type = type;
		this.pattern = pattern;
		this.caseSensitive = caseSensitive;
		this.desc = desc;
		this.ddlWrappers = ddlWrappers;
	}

	public Type getType() {
		return type;
	}

	public String getPattern() {
		return pattern;
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	public String getDesc() {
		return desc;
	}

	public Class<? extends DDLWrapper<?>>[] getDdlWrappers() {
		return ddlWrappers;
	}

	public enum Type{
		ADD_COLUMN,
		CHANGE_COLUMN,
		MODIFY_COLUMN,
		RENAME_COLUMN,
		DROP_COLUMN,
	}
}

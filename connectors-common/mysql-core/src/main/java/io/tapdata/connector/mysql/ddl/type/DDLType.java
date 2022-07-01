package io.tapdata.connector.mysql.ddl.type;

import io.tapdata.connector.mysql.ddl.DDLWrapper;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 17:17
 **/
public class DDLType {
	protected String pattern;
	protected boolean caseSensitive;
	protected String desc;
	protected Class<? extends DDLWrapper>[] ddlWrappers;

	public DDLType(String pattern, boolean caseSensitive, String desc, Class<? extends DDLWrapper>... ddlWrappers) {
		this.pattern = pattern;
		this.caseSensitive = caseSensitive;
		this.desc = desc;
		this.ddlWrappers = ddlWrappers;
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

	public Class<? extends DDLWrapper>[] getDdlWrappers() {
		return ddlWrappers;
	}
}

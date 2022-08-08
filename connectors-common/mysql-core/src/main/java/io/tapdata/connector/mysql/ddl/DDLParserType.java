package io.tapdata.connector.mysql.ddl;

import io.tapdata.connector.mysql.ddl.parser.CCJSqlParser;
import io.tapdata.connector.mysql.ddl.type.CCJWrapperType;
import io.tapdata.connector.mysql.ddl.type.WrapperType;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 14:34
 **/
public enum DDLParserType {
	CCJ_SQL_PARSER("com.github.jsqlparser", CCJSqlParser.class, CCJWrapperType.class),
	;

	private final String desc;
	private final Class<? extends DDLParser<?>> parserClz;
	private final Class<? extends WrapperType> wrapperType;

	DDLParserType(String desc, Class<? extends DDLParser<?>> parserClz,Class<? extends WrapperType> wrapperType) {
		this.desc = desc;
		this.parserClz = parserClz;
		this.wrapperType = wrapperType;
	}

	public String getDesc() {
		return desc;
	}

	public Class<? extends DDLParser<?>> getParserClz() {
		return parserClz;
	}

	public Class<? extends WrapperType> getWrapperType() {
		return wrapperType;
	}
}

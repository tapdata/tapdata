package io.tapdata.connector.mariadb.ddl.parser;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.commons.lang3.StringUtils;


public class CCJSqlParser extends BaseDDLParser<Statement> {
	@Override
	public Statement parse(String ddl) throws Throwable {
		if (StringUtils.isBlank(ddl)) {
			return null;
		}
		Statement statement = CCJSqlParserUtil.parse(ddl);
		return statement;
	}
}

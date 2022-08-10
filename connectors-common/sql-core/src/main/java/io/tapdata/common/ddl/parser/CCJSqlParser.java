package io.tapdata.common.ddl.parser;

import io.tapdata.kit.EmptyKit;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 14:32
 **/
public class CCJSqlParser extends BaseDDLParser<Statement> {
    @Override
    public Statement parse(String ddl) throws Throwable {
        if (EmptyKit.isBlank(ddl)) {
            return null;
        }
        return CCJSqlParserUtil.parse(ddl);
    }
}

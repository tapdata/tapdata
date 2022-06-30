package io.tapdata.cdc.ddl.sql.parser;

import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.events.AddField;
import io.tapdata.cdc.ddl.sql.SqlParser;
import io.tapdata.cdc.ddl.utils.StringReader;

import java.util.List;
import java.util.function.Consumer;

/**
 * 事件解析 - 添加列
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 上午1:30 Create
 */
public class AddColumn implements Table.Child {

	@Override
	public boolean check(SqlParser sqlParser, StringReader sr, Consumer<DdlEvent> outConsumer, List<String> namespace) {
		if (sr.equalsIgnoreCaseAndMove("add") && sr.nextAndSkip(sqlParser::spaceFn)) {
			// column 属性可以没有
			if (sr.equalsIgnoreCaseAndMove("column")) {
				sr.nextAndSkipFail2Ex(sqlParser::spaceFn, "Illegal char");
			}
			String fieldName = sqlParser.loadName(sr);
			sr.nextAndSkipFail2Ex(sqlParser::spaceFn, "Illegal char");
			String type = sr.substring(sr.position());
			outConsumer.accept(new AddField(sr.data(), namespace, fieldName, type));
			return true;
		}
		return false;
	}
}

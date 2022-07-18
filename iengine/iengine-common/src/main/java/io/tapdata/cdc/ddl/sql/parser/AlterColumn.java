package io.tapdata.cdc.ddl.sql.parser;

import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.events.AlterField;
import io.tapdata.cdc.ddl.sql.SqlParser;
import io.tapdata.cdc.ddl.utils.StringReader;

import java.util.List;
import java.util.function.Consumer;

/**
 * 事件解析 - 修改列
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 下午10:01 Create
 */
public class AlterColumn implements Table.Child {

	@Override
	public boolean check(SqlParser sqlParser, StringReader sr, Consumer<DdlEvent> outConsumer, List<String> namespace) {
		if (sr.equalsIgnoreCaseAndMove("alter") && sr.nextAndSkip(sqlParser::spaceFn)) {
			if (sr.equalsIgnoreCaseAndMove("column") && sr.nextAndSkip(sqlParser::spaceFn)) {
				String fieldName = sqlParser.loadName(sr);
				sr.nextAndSkipFail2Ex(sqlParser::spaceFn, "Illegal char");
				String type = sr.substring(sr.position());
				outConsumer.accept(new AlterField(sr.data(), namespace, fieldName, type));
				return true;
			}
			throw sr.ex("Illegal char");
		}
		return false;
	}
}

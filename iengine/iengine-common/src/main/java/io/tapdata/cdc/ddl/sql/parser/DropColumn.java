package io.tapdata.cdc.ddl.sql.parser;

import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.events.DropField;
import io.tapdata.cdc.ddl.sql.SqlParser;
import io.tapdata.cdc.ddl.utils.StringReader;

import java.util.List;
import java.util.function.Consumer;

/**
 * 事件解析 - 删除列
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 上午1:26 Create
 */
public class DropColumn implements Table.Child {

	@Override
	public boolean check(SqlParser sqlParser, StringReader sr, Consumer<DdlEvent> outConsumer, List<String> namespace) {
		if (sr.equalsIgnoreCaseAndMove("drop") && sr.nextAndSkip(sqlParser::spaceFn)) {
			if (sr.equalsIgnoreCaseAndMove("column") && sr.nextAndSkip(sqlParser::spaceFn)) {
				outConsumer.accept(new DropField(sr.data(), namespace, sqlParser.loadName(sr)));
				return true;
			}
		}
		return false;
	}
}

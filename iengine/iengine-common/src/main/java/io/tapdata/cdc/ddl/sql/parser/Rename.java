package io.tapdata.cdc.ddl.sql.parser;

import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.events.RenameField;
import io.tapdata.cdc.ddl.events.RenameStruct;
import io.tapdata.cdc.ddl.sql.SqlParser;
import io.tapdata.cdc.ddl.utils.StringReader;

import java.util.List;
import java.util.function.Consumer;

/**
 * 事件解析 - 重命名
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 上午1:26 Create
 */
public class Rename implements Table.Child {

	@Override
	public boolean check(SqlParser sqlParser, StringReader sr, Consumer<DdlEvent> outConsumer, List<String> namespace) {
		if (sr.equalsIgnoreCaseAndMove("rename") && sr.nextAndSkip(sqlParser::spaceFn)) {
			if (sr.equalsIgnoreCaseAndMove("to") && sr.nextAndSkip(sqlParser::spaceFn)) {
				outConsumer.accept(new RenameStruct(sr.data(), namespace, sqlParser.loadName(sr)));
				return true;
			} else if (sr.equalsIgnoreCaseAndMove("column")) sr.nextAndSkip(sqlParser::spaceFn);
			String fileName = sqlParser.loadName(sr);
			if (sr.nextAndSkip(sqlParser::spaceFn) && sr.equalsIgnoreCaseAndMove("to") && sr.nextAndSkip(sqlParser::spaceFn)) {
				outConsumer.accept(new RenameField(sr.data(), namespace, fileName, sqlParser.loadName(sr)));
				return true;
			}
		}
		return false;
	}
}

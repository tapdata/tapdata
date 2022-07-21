package io.tapdata.cdc.ddl.sql.parser;

import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.sql.SqlParser;
import io.tapdata.cdc.ddl.utils.StringReader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 事件解析 - 表
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 上午1:17 Create
 */
public class Table {

	private String prefix;
	private List<Child> children = new ArrayList<>();

	public Table(String prefix) {
		this.prefix = prefix;
	}

	public boolean check(SqlParser sqlParser, StringReader sr, Consumer<DdlEvent> outConsumer) {
		if (sr.equalsIgnoreCaseAndMove(prefix) && sr.nextAndSkip(sqlParser::spaceFn)) {
			if (sr.equalsIgnoreCaseAndMove("table") && sr.nextAndSkip(sqlParser::spaceFn)) {
				List<String> namespace = sqlParser.loadNames(sr);
				for (Child child : children) {
					if (child.check(sqlParser, sr, outConsumer, namespace)) return true;
				}
			}
		}
		return false;
	}

	public Table add(Child child) {
		children.add(child);
		return this;
	}

	interface Child {
		boolean check(SqlParser sqlParser, StringReader sr, Consumer<DdlEvent> outConsumer, List<String> namespace);
	}
}

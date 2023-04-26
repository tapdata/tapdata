package io.tapdata.cdc.ddl.mysql;

import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.DdlParser;
import io.tapdata.cdc.ddl.exception.DdlException;
import io.tapdata.cdc.ddl.exception.DdlParserException;
import io.tapdata.cdc.ddl.sql.SqlParser;
import io.tapdata.cdc.ddl.sql.parser.AddColumn;
import io.tapdata.cdc.ddl.sql.parser.DropColumn;
import io.tapdata.cdc.ddl.sql.parser.DropTable;
import io.tapdata.cdc.ddl.sql.parser.Rename;
import io.tapdata.cdc.ddl.sql.parser.Table;
import io.tapdata.cdc.ddl.utils.StringReader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 源DDL转换器 - MySQL
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/10 下午7:12 Create
 */
public class MysqlDdlParser implements DdlParser<String, DdlEvent> {

	private static final MysqlDdlParser INSTANCE = new MysqlDdlParser();

	public static MysqlDdlParser ins() {
		return INSTANCE;
	}

	private SqlParser sqlParser = new SqlParser();
	private List<Table> tables = new ArrayList<>();

	private MysqlDdlParser() {
		tables.add(new Table("alter")
				.add(new DropColumn())
				.add(new AddColumn())
				.add(new Rename())
		);
		tables.add(new Table("create"));
		tables.add(new DropTable());
	}

	@Override
	public void parseDDL(String in, Consumer<DdlEvent> outConsumer) {
		if (null == in || (in = in.trim()).isEmpty()) throw new DdlException("DDL is empty");

		StringReader sr = new StringReader(in) {
			@Override
			public RuntimeException ex(String msg) {
				return new DdlParserException(msg + ", position " + position(), data());
			}
		};
		for (Table table : tables) {
			if (table.check(sqlParser, sr, outConsumer)) return;
		}
		throw sr.ex("Unrealized ddl operator");
	}
}

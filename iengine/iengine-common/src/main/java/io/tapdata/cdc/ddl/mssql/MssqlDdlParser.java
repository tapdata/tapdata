package io.tapdata.cdc.ddl.mssql;

import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.DdlParser;
import io.tapdata.cdc.ddl.exception.DdlException;
import io.tapdata.cdc.ddl.exception.DdlParserException;
import io.tapdata.cdc.ddl.sql.SqlParser;
import io.tapdata.cdc.ddl.sql.parser.AddColumn;
import io.tapdata.cdc.ddl.sql.parser.AlterColumn;
import io.tapdata.cdc.ddl.sql.parser.DropColumn;
import io.tapdata.cdc.ddl.sql.parser.DropTable;
import io.tapdata.cdc.ddl.sql.parser.Rename;
import io.tapdata.cdc.ddl.sql.parser.Table;
import io.tapdata.cdc.ddl.utils.StringReader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 源DDL转换器 - SQLServer
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/10 下午6:46 Create
 */
public class MssqlDdlParser implements DdlParser<String, DdlEvent> {

	private static final MssqlDdlParser INSTANCE = new MssqlDdlParser();

	public static MssqlDdlParser ins() {
		return INSTANCE;
	}

	private SqlParser sqlParser;
	private List<Table> tables = new ArrayList<>();

	private MssqlDdlParser() {
		this.sqlParser = new SqlParser('\\', '[', ']', '.', '\'');
		tables.add(new Table("alter")
				.add(new DropColumn())
				.add(new AddColumn())
				.add(new Rename())
				.add(new AlterColumn())
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

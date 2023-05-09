package io.tapdata.ddlp.utils;

import io.tapdata.ddlp.DDLEvent;
import io.tapdata.ddlp.events.AddField;
import io.tapdata.ddlp.events.AddFieldDefault;
import io.tapdata.ddlp.events.AlterField;
import io.tapdata.ddlp.events.DropField;
import io.tapdata.ddlp.events.UnSupported;
import io.tapdata.ddlp.exception.DDLException;
import io.tapdata.ddlp.parsers.DDLParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * DDL解析器 - SQL实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午3:58 Create
 */
public abstract class SqlDDLParser implements DDLParser<String, DDLEvent> {

	private Logger logger = LogManager.getLogger(getClass());

	protected Function<Character, Boolean> spaceFn = c -> {
		switch (c) {
			case ' ':
			case '\n':
			case '\r':
			case '\t':
				return true;
			default:
				return false;
		}
	};

	protected SqlDDLParser() {
	}

	@Override
	public void ddl2Event(String in, Consumer<DDLEvent> out) {
		if (null == in || (in = in.trim()).isEmpty()) throw new DDLException("DDL is empty");

		CharReader reader = new CharReader(in) {
			@Override
			public RuntimeException ex(String msg) {
				return new DDLException(msg + ", position " + pos() + ": " + data());
			}
		};

		try {
			String op = reader.readNotIn(spaceFn);
			if ("alter".equalsIgnoreCase(op) && reader.nextAndSkipIn(spaceFn)) {
				String type = reader.readNotIn(spaceFn);
				if ("table".equalsIgnoreCase(type) && reader.nextAndSkipIn(spaceFn)) {
					alertTable(reader, out, loadNamespace(reader));
					return;
				}
			}
			out.accept(new UnSupported(reader.data(), reader.pos(), null));
		} catch (Exception e) {
			out.accept(new UnSupported(reader.data(), reader.pos(), e.getMessage()));
			logger.warn("Nonsupport ddl: " + reader.toString(), e);
		}
	}

	/**
	 * 加载名称
	 *
	 * @param reader 读取器
	 * @return 名称，为空时表示读取不到
	 */
	protected String loadName(CharReader reader) {
		switch (reader.current()) {
			case '"':
				return reader.readInQuote('"', '"');
			default:
				return reader.readNotIn(spaceFn);
		}
	}

	/**
	 * 加载命名空间
	 *
	 * @param reader 读取器
	 * @return 命名空间
	 */
	protected List<String> loadNamespace(CharReader reader) {
		String tmp;
		List<String> namespace = new ArrayList<>();
		while (true) {
			tmp = loadName(reader);
			if (null == tmp) throw reader.ex("Name is null");
			namespace.add(tmp);
			if (!reader.nextAndSkipIn(spaceFn)) break;
			if ('.' != reader.current()) break;
			reader.nextAndSkipIn(spaceFn, "Illegal char");
		}
		return namespace;
	}

	/**
	 * 修改表解析逻辑
	 *
	 * @param reader    读取器
	 * @param out       输出
	 * @param namespace 命名空间
	 */
	protected void alertTable(CharReader reader, Consumer<DDLEvent> out, List<String> namespace) {
		String op = reader.readNotIn(spaceFn);
		if (reader.nextAndSkipIn(spaceFn)) {
			switch (reader.checkIn2Index(true, true, "constraint", "column", "default")) {
				case 0:
					throw reader.ex("Temporary unsupported constraint ddl");
				case 1:
					reader.nextAndSkipIn(spaceFn, "Bad " + op + " column");
					break;
				case 2:
					alterColumnDefault(reader, out, namespace);
					return;
				default:
					break;
			}

			String fieldName = loadName(reader);
			if ("add".equalsIgnoreCase(op) && reader.nextAndSkipIn(spaceFn)) {
				addColumn(reader, out, namespace, fieldName);
				return;
			} else if ("alter".equalsIgnoreCase(op) && reader.nextAndSkipIn(spaceFn)) {
				alterColumn(reader, out, namespace, fieldName);
				return;
			} else if ("drop".equalsIgnoreCase(op)) {
				dropColumn(reader, out, namespace, fieldName);
				return;
			}
		}
		throw reader.ex("Not support AlterTable");
	}

	protected void addColumn(CharReader reader, Consumer<DDLEvent> out, List<String> namespace, String fieldName) {
		String type = reader.substring(reader.pos());
		out.accept(new AddField(reader.data(), namespace, fieldName, type));
	}

	protected void alterColumn(CharReader reader, Consumer<DDLEvent> out, List<String> namespace, String fieldName) {
		String type = reader.substring(reader.pos());
		out.accept(new AlterField(reader.data(), namespace, fieldName, type));
	}

	protected void alterColumnDefault(CharReader reader, Consumer<DDLEvent> out, List<String> namespace) {
		if (reader.nextAndSkipIn(spaceFn)) {
			String value;
			if (reader.is('\'')) {
				int begin = reader.pos();
				reader.readInQuote('\'', '\'', '\'', '\'');
				value = reader.substring(begin, reader.pos() + 1);
			} else {
				value = reader.readNotIn(spaceFn);
			}

			if (reader.nextAndSkipIn(spaceFn)) {
				switch (reader.checkIn2Index(true, true, "for")) {
					case 0:
						if (reader.nextAndSkipIn(spaceFn)) {
							out.accept(new AddFieldDefault(reader.data(), namespace, loadName(reader), value));
							return;
						}
						break;
					default:
						throw reader.ex("Not found column name");
				}
			}
		}
		throw reader.ex("Bad add default");
	}

	protected void dropColumn(CharReader reader, Consumer<DDLEvent> out, List<String> namespace, String fieldName) {
		out.accept(new DropField(reader.data(), namespace, fieldName));
	}
}

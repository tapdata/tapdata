package io.tapdata.ddlp;

import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.ddlp.converters.DDLConverter;
import io.tapdata.ddlp.exception.DDLException;
import io.tapdata.ddlp.parsers.DDLParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * DDL处理器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午12:45 Create
 */
public class DDLProcessor {
	private static final Map<DatabaseTypeEnum, DDLParser> parsers = new HashMap<>();
	private static final Map<DatabaseTypeEnum, DDLConverter> converters = new HashMap<>();

	/**
	 * 获取解析器
	 *
	 * @param databaseType 数据库类型
	 * @param <I>          输入类型
	 * @param <O>          输出类型
	 * @return 解析器
	 */
	public static <I, O extends DDLEvent> DDLParser<I, O> getParser(DatabaseTypeEnum databaseType) {
		synchronized (parsers) {
			DDLParser parser = parsers.get(databaseType);
			if (null == parser) {
				try {
					parser = BeanUtil.newDatabaseImpl(databaseType, DDLParser.class);
					parsers.put(databaseType, parser);
				} catch (Exception e) {
					throw new DDLException(databaseType.getType() + " can't find DDL parser: " + e.getMessage(), e);
				}
			}
			if (null == parser) {
				throw new DDLException(databaseType.getType() + " can't find DDL parser.");
			}
			return parser;
		}
	}

	/**
	 * 获取转换器
	 *
	 * @param databaseType 数据库类型
	 * @param <I>          输入类型
	 * @param <O>          输出类型
	 * @return 转换器
	 */
	public static <I extends DDLEvent, O> DDLConverter<I, O> getConverter(DatabaseTypeEnum databaseType) {
		synchronized (converters) {
			DDLConverter converter = converters.get(databaseType);
			if (null == converter) {
				try {
					converter = BeanUtil.newDatabaseImpl(databaseType, DDLConverter.class);
					converters.put(databaseType, converter);
				} catch (Exception e) {
					throw new DDLException(databaseType.getType() + " can't find DDL converter: " + e.getMessage(), e);
				}
			}
			if (null == converter) {
				throw new DDLException(databaseType.getType() + " can't find DDL converter.");
			}
			return converter;
		}
	}

	/**
	 * DDL解析
	 *
	 * @param ddl ddl
	 * @return 事件
	 */
	public static <I, O extends DDLEvent> List<O> parse(DatabaseTypeEnum databaseType, I ddl) {
		return DDLProcessor.<I, O>getParser(databaseType).ddl2Event(ddl);
	}

	/**
	 * DDL解析
	 *
	 * @param databaseType 数据库类型
	 * @param ddl          DDL
	 * @param out          事件消费
	 * @param <I>          输入类型
	 * @param <O>          输出类型
	 */
	public static <I, O extends DDLEvent> void parse(DatabaseTypeEnum databaseType, I ddl, Consumer<O> out) {
		DDLProcessor.<I, O>getParser(databaseType).ddl2Event(ddl, out);
	}

	/**
	 * DDL转换
	 *
	 * @param event 事件
	 * @return ddl
	 */
	public static <I extends DDLEvent, O> List<O> convert(DatabaseTypeEnum databaseType, I event) {
		return DDLProcessor.<I, O>getConverter(databaseType).event2ddl(event);
	}

	/**
	 * DDL转换
	 *
	 * @param databaseType 数据库类型
	 * @param event        事件
	 * @param out          DDL消费
	 * @param <I>          输入类型
	 * @param <O>          输出类型
	 */
	public static <I extends DDLEvent, O> void convert(DatabaseTypeEnum databaseType, I event, Consumer<O> out) {
		DDLProcessor.<I, O>getConverter(databaseType).event2ddl(event, out);
	}

	public static void main(String[] args) {
		String ddl = "";
		for (DDLEvent event : DDLProcessor.parse(DatabaseTypeEnum.MYSQL, ddl)) {
			for (String sql : DDLProcessor.<DDLEvent, String>convert(DatabaseTypeEnum.MYSQL, event)) {
				System.out.println(sql);
			}
		}
	}
}

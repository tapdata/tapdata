package io.tapdata.ddlp.utils;

import io.tapdata.ddlp.DDLEvent;
import io.tapdata.ddlp.converters.DDLConverter;
import io.tapdata.ddlp.events.AbsStruct;
import io.tapdata.ddlp.events.AddField;
import io.tapdata.ddlp.events.AddFieldDefault;
import io.tapdata.ddlp.events.AlterField;
import io.tapdata.ddlp.events.DropField;
import io.tapdata.ddlp.exception.DDLException;

import java.util.List;
import java.util.function.Consumer;

/**
 * DDL转换器 - SQL实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午4:00 Create
 */
public abstract class SqlDDLConverter implements DDLConverter<DDLEvent, String> {

	protected SqlDDLConverter() {
	}

	@Override
	public void event2ddl(DDLEvent in, Consumer<String> out) {
		switch (in.getOp()) {
			case AddField:
				convert((AddField) in, out);
				break;
			case DropField:
				convert((DropField) in, out);
				break;
			case AddFieldDefault:
				convert((AddFieldDefault) in, out);
				break;
			case AlterField:
				convert((AlterField) in, out);
				break;
			default:
				throw new DDLException("Not support ddl operator '" + in.getOp() + "': " + in);
		}
	}

	protected String nameWrap(String val) {
		if (null == val) return null;

		char c, nameBegin = '"', nameEnd = '"', escape = '\\';
		StringBuilder buf = new StringBuilder();
		buf.append(nameBegin);
		for (int i = 0, len = val.length(); i < len; i++) {
			c = val.charAt(i);
			if (nameBegin == c || escape == c) {
				buf.append('\\');
			}
			buf.append(c);
		}
		buf.append(nameEnd);
		return buf.toString();
	}

	protected String tableName(AbsStruct struct) {
		List<String> namespace = struct.getNamespace();
		if (null == namespace || namespace.isEmpty()) return null;
		return namespace.get(namespace.size() - 1);
	}

	/**
	 * 包裹表名，目标库过滤 schema 的处理
	 *
	 * @param struct 结构信息
	 * @return 被包裹的表名
	 */
	protected String wrapTableName(AbsStruct struct) {
		String tableName = tableName(struct);
		if (null == tableName) return null;
		return nameWrap(tableName);
	}

	protected String wrapNamespace(AbsStruct struct) {
		StringBuilder buf = new StringBuilder();
		List<String> namespace = struct.getNamespace();
		if (null == namespace) return null;
		for (String n : namespace) {
			if (null == n) continue;
			buf.append(nameWrap(n)).append(".");
		}
		buf.setLength(buf.length() - 1);
		return buf.toString();
	}

	protected void convert(AddField event, Consumer<String> outConsumer) {
		outConsumer.accept("alter table " +
				wrapNamespace(event) +
				" add " +
				nameWrap(event.getName()) +
				" " + event.getType()
		);
	}

	protected void convert(AddFieldDefault event, Consumer<String> outConsumer) {
		outConsumer.accept("alter table " + wrapNamespace(event) +
				" add default " + event.getValue() +
				" for " + nameWrap(event.getName())
		);
	}

	protected void convert(DropField event, Consumer<String> outConsumer) {
		outConsumer.accept("alter table " +
				wrapNamespace(event) +
				" drop column " +
				nameWrap(event.getName())
		);
	}

	protected void convert(AlterField event, Consumer<String> outConsumer) {
		outConsumer.accept("alter table " +
				wrapNamespace(event) +
				" alter column " +
				nameWrap(event.getName()) +
				" " + event.getType()
		);
	}

}

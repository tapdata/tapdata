package io.tapdata.cdc.ddl.mssql;

import io.tapdata.cdc.ddl.DdlConverter;
import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.events.AbsStruct;
import io.tapdata.cdc.ddl.events.AddField;
import io.tapdata.cdc.ddl.events.AlterField;
import io.tapdata.cdc.ddl.events.DropField;
import io.tapdata.cdc.ddl.events.DropStruct;
import io.tapdata.cdc.ddl.events.RenameField;
import io.tapdata.cdc.ddl.exception.DdlConverterException;
import io.tapdata.cdc.ddl.sql.SqlConverter;

import java.util.List;
import java.util.function.Consumer;

/**
 * 目标DDL转换器 - SQLServer
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/11 下午3:26 Create
 */
public class MssqlDdlConverter implements DdlConverter<DdlEvent, String> {

	private static final MssqlDdlConverter INSTANCE = new MssqlDdlConverter(new SqlConverter('\\', '[', ']', '.', '\''));

	public static MssqlDdlConverter ins() {
		return INSTANCE;
	}

	private SqlConverter converter;

	protected MssqlDdlConverter(SqlConverter converter) {
		this.converter = converter;
	}

	@Override
	public void convertDDL(DdlEvent in, Consumer<String> outConsumer) {
		switch (in.getOp()) {
			case AddField:
				convert((AddField) in, outConsumer);
				break;
			case DropField:
				convert((DropField) in, outConsumer);
				break;
			case RenameField:
				convert((RenameField) in, outConsumer);
				break;
			case AlterField:
				convert((AlterField) in, outConsumer);
				break;
			case DropStruct:
				convert((DropStruct) in, outConsumer);
				break;
			default:
				throw new DdlConverterException("Not support ddl operator '" + in.getOp() + "'", in);
		}
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
		return converter.nameWrap(tableName);
	}

	protected void convert(AddField event, Consumer<String> outConsumer) {
		outConsumer.accept("alter table " +
				wrapTableName(event) +
				" add column " +
				converter.nameWrap(event.getName()) +
				" " + event.getType()
		);
	}

	protected void convert(DropField event, Consumer<String> outConsumer) {
		outConsumer.accept("alter table " +
				wrapTableName(event) +
				" drop column " +
				converter.nameWrap(event.getName())
		);
	}

	protected void convert(RenameField event, Consumer<String> outConsumer) {
		outConsumer.accept("alter table " +
				wrapTableName(event) +
				" rename column " +
				converter.nameWrap(event.getName()) +
				" to " +
				converter.nameWrap(event.getRename())
		);
	}

	protected void convert(AlterField event, Consumer<String> outConsumer) {
		outConsumer.accept("alter table " +
				wrapTableName(event) +
				" alter column " +
				converter.nameWrap(event.getName()) +
				" " + event.getType()
		);
	}

	protected void convert(DropStruct event, Consumer<String> outConsumer) {
		outConsumer.accept("drop table " +
				wrapTableName(event)
		);
	}
}

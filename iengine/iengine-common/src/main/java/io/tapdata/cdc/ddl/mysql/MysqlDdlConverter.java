package io.tapdata.cdc.ddl.mysql;

import io.tapdata.cdc.ddl.DdlConverter;
import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.exception.DdlConverterException;
import io.tapdata.cdc.ddl.sql.SqlConverter;

import java.util.function.Consumer;

/**
 * 目标DDL转换器 - MySQL
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/10 下午7:14 Create
 */
public class MysqlDdlConverter implements DdlConverter<DdlEvent, String> {

	private static final MysqlDdlConverter INSTANCE = new MysqlDdlConverter(new SqlConverter());

	public static MysqlDdlConverter ins() {
		return INSTANCE;
	}

	private SqlConverter converter;

	protected MysqlDdlConverter(SqlConverter converter) {
		this.converter = converter;
	}

	@Override
	public void convertDDL(DdlEvent in, Consumer<String> outConsumer) {
		switch (in.getOp()) {
			default:
				throw new DdlConverterException("Not support ddl operator '" + in.getOp() + "'", in);
		}
	}
}

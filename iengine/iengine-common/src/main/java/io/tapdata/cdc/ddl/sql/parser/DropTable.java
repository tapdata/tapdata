package io.tapdata.cdc.ddl.sql.parser;

import io.tapdata.cdc.ddl.events.DropStruct;

/**
 * 事件解析 - 删除表
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 上午3:31 Create
 */
public class DropTable extends Table {
	public DropTable() {
		super("drop");
		add((sqlParser, sr, outConsumer, namespace) -> {
			outConsumer.accept(new DropStruct(sr.data(), namespace));
			return true;
		});
	}
}

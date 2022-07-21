package io.tapdata.ddlp.parsers;

import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.annotation.DatabaseTypeAnnotation;
import io.tapdata.ddlp.utils.CharReader;
import io.tapdata.ddlp.utils.SqlDDLParser;

/**
 * DDL解析器 - MySQL
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午3:06 Create
 */
@DatabaseTypeAnnotation(type = DatabaseTypeEnum.MYSQL)
public class MysqlDDLParser extends SqlDDLParser {

	/**
	 * 加载名称
	 *
	 * @param reader 读取器
	 * @return 名称，为空时表示读取不到
	 */
	protected String loadName(CharReader reader) {
		switch (reader.current()) {
			case '`':
				return reader.readInQuote('`', '`');
			default:
				return reader.readNotIn(spaceFn);
		}
	}
}

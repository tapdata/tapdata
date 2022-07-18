package io.tapdata.ddlp.converters;

import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.annotation.DatabaseTypeAnnotation;
import io.tapdata.ddlp.utils.SqlDDLConverter;

/**
 * DDL转换器 - MySQL
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午3:40 Create
 */
@DatabaseTypeAnnotation(type = DatabaseTypeEnum.MYSQL)
public class MysqlDDLConverter extends SqlDDLConverter {

	@Override
	protected String nameWrap(String val) {
		if (null == val) return null;

		char c, nameBegin = '`', nameEnd = '`', escape = '\\';
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
}

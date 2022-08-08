package io.tapdata.connector.mysql.ddl;

import io.tapdata.connector.mysql.ddl.type.DDLType;
import io.tapdata.connector.mysql.ddl.type.WrapperType;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.List;
import java.util.regex.Pattern;

/**
 * @author samuel
 * @Description
 * @create 2022-06-30 10:09
 **/
public class DDLFilter {

	public static DDLType testAndGetType(DDLParserType ddlParserType, String ddl) {
		Class<? extends WrapperType> wrapperType = ddlParserType.getWrapperType();
		WrapperType wrapperBean = InstanceFactory.bean(wrapperType);
		List<DDLType> ddlTypes = wrapperBean.getDdlTypes();
		for (DDLType ddlType : ddlTypes) {
			String pattern = ddlType.getPattern();
			Pattern compile = ddlType.isCaseSensitive() ? Pattern.compile(pattern) : Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			if (compile.matcher(ddl).matches()) {
				return ddlType;
			}
		}
		return null;
	}
}

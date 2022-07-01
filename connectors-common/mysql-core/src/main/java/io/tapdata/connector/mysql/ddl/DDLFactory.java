package io.tapdata.connector.mysql.ddl;

import io.tapdata.connector.mysql.ddl.type.WrapperType;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 12:17
 **/
public class DDLFactory {
	public static void parse(DDLParserType ddlParserType, String ddl, Consumer<TapDDLEvent> consumer) {
		Class<? extends DDLParser<?>> parserClz = ddlParserType.getParserClz();
		Class<? extends WrapperType> wrapperType = ddlParserType.getWrapperType();
		WrapperType wrapperBean = InstanceFactory.bean(wrapperType);

	}
}

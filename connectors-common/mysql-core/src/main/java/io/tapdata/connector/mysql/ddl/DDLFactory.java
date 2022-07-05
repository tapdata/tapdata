package io.tapdata.connector.mysql.ddl;

import io.tapdata.connector.mysql.ddl.type.DDLType;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;

import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 12:17
 **/
public class DDLFactory {
	public static <E> void ddlToTapDDLEvent(DDLParserType ddlParserType, String ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) throws Throwable {
		DDLType ddlType = DDLFilter.testAndGetType(ddlParserType, ddl);
		if (null == ddlType) {
			return;
		}
		Class<? extends DDLParser<E>> parserClz = (Class<? extends DDLParser<E>>) ddlParserType.getParserClz();
		DDLParser<E> ddlParser = InstanceFactory.bean(parserClz);
		E parseResult = ddlParser.parse(ddl);
		Class<? extends DDLWrapper<E>>[] ddlWrappers = (Class<? extends DDLWrapper<E>>[]) ddlType.getDdlWrappers();
		for (Class<? extends DDLWrapper<E>> ddlWrapper : ddlWrappers) {
			DDLWrapper<E> ddlWrapperBean = InstanceFactory.bean(ddlWrapper);
			ddlWrapperBean.wrap(parseResult, tableMap, consumer);
		}
	}
}

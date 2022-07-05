package io.tapdata.connector.mysql.ddl.wrapper.ccj;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import net.sf.jsqlparser.statement.alter.Alter;

import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-05 10:47
 **/
public class CCJDropColumnDDLWrapper extends CCJBaseDDLWrapper {
	@Override
	public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) throws Throwable {
		verifyAlter(ddl);
	}
}

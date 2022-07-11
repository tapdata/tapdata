package io.tapdata.connector.mysql.ddl.wrapper.ccj;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-05 10:47
 **/
public class CCJDropColumnDDLWrapper extends CCJBaseDDLWrapper {
	@Override
	public List<Capability> getCapabilities() {
		return Collections.singletonList(Capability.create(ConnectionOptions.DDL_DROP_FIELD_EVENT));
	}

	@Override
	public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) throws Throwable {
		verifyAlter(ddl);
		String tableName = getTableName(ddl);
		List<AlterExpression> alterExpressions = ddl.getAlterExpressions();
		for (AlterExpression alterExpression : alterExpressions) {
			if (alterExpression.getOperation() != AlterOperation.DROP) {
				continue;
			}
			TapDropFieldEvent tapDropFieldEvent = new TapDropFieldEvent();
			tapDropFieldEvent.setTableId(tableName);
			tapDropFieldEvent.setFieldName(alterExpression.getColumnName());
			consumer.accept(tapDropFieldEvent);
		}
	}
}

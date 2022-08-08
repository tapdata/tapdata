package io.tapdata.connector.mysql.ddl.wrapper.ccj;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-04 17:52
 **/
public class CCJAlterColumnNameDDLWrapper extends CCJBaseDDLWrapper {
	@Override
	public List<Capability> getCapabilities() {
		return Collections.singletonList(Capability.create(ConnectionOptions.DDL_ALTER_FIELD_NAME_EVENT).type(Capability.TYPE_DDL));
	}

	@Override
	public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) throws Throwable {
		verifyAlter(ddl);
		TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
		String tableName = getTableName(ddl);
		tapAlterFieldNameEvent.setTableId(tableName);
		List<AlterExpression> alterExpressions = ddl.getAlterExpressions();
		if (null == alterExpressions || alterExpressions.size() <= 0) {
			return;
		}
		AlterExpression alterExpression = alterExpressions.get(0);
		if (alterExpression.getOperation() == AlterOperation.CHANGE) {
			List<AlterExpression.ColumnDataType> colDataTypeList = alterExpression.getColDataTypeList();
			if (null == colDataTypeList || colDataTypeList.size() <= 0) {
				return;
			}
			AlterExpression.ColumnDataType columnDataType = colDataTypeList.get(0);
			if (null == columnDataType || StringUtils.isBlank(columnDataType.getColumnName())) {
				return;
			}
			tapAlterFieldNameEvent.nameChange(ValueChange.create(alterExpression.getColumnOldName(), columnDataType.getColumnName()));
			consumer.accept(tapAlterFieldNameEvent);
		} else if (alterExpression.getOperation() == AlterOperation.RENAME) {
			String columnName = alterExpression.getColumnName();
			String columnOldName = alterExpression.getColumnOldName();
			if (StringUtils.isBlank(columnName) || StringUtils.isBlank(columnOldName)) {
				return;
			}
			tapAlterFieldNameEvent.nameChange(ValueChange.create(columnOldName, columnName));
			consumer.accept(tapAlterFieldNameEvent);
		}
	}
}

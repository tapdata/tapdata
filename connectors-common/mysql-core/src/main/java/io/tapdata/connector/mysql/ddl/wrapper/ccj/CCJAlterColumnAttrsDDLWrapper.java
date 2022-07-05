package io.tapdata.connector.mysql.ddl.wrapper.ccj;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-04 17:45
 **/
public class CCJAlterColumnAttrsDDLWrapper extends CCJBaseDDLWrapper {
	@Override
	public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) throws Throwable {
		verifyAlter(ddl);
		String tableName = getTableName(ddl);
		TapTable tapTable = null != tableMap ? tableMap.get(tableName) : null;
		List<AlterExpression> alterExpressions = ddl.getAlterExpressions();
		if (null == alterExpressions || alterExpressions.size() <= 0) {
			return;
		}
		for (AlterExpression alterExpression : alterExpressions) {
			List<AlterExpression.ColumnDataType> colDataTypeList = alterExpression.getColDataTypeList();
			for (AlterExpression.ColumnDataType columnDataType : colDataTypeList) {
				TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = new TapAlterFieldAttributesEvent();
				tapAlterFieldAttributesEvent.setTableId(tableName);
				String columnName = columnDataType.getColumnName();
				tapAlterFieldAttributesEvent.fieldName(columnName);
				List<String> columnSpecs = columnDataType.getColumnSpecs();
				String preSpec = "";
				for (String columnSpec : columnSpecs) {
					switch (columnSpec) {
						case "not":
						case "NOT":
						case "default":
						case "DEFAULT":
						case "comment":
						case "COMMENT":
							preSpec = columnSpec;
							break;
						case "null":
						case "NULL":
							boolean nullable = !"not".equals(preSpec);
							tapAlterFieldAttributesEvent.notNull(ValueChange.create(nullable));
							preSpec = "";
							break;
						case "key":
						case "KEY":
							if (StringUtils.equalsAny(preSpec, "", "primary", "PRIMARY")) {
								int primaryPos = 1;
								if (null != tapTable) {
									primaryPos = tapTable.getMaxPKPos() + 1;
								}
								tapAlterFieldAttributesEvent.primaryChange(ValueChange.create(primaryPos));
								preSpec = "";
							}
							break;
						default:
							if (StringUtils.equalsAnyIgnoreCase(preSpec, "default")) {
								tapAlterFieldAttributesEvent.defaultChange(ValueChange.create(columnSpec));
								preSpec = "";
							} else if (StringUtils.equalsAnyIgnoreCase(preSpec, "comment")) {
								tapAlterFieldAttributesEvent.comment(ValueChange.create(removeFirstAndLastApostrophe(columnSpec)));
								preSpec = "";
							}
							break;
					}
				}
				ColDataType colDataType = columnDataType.getColDataType();
				String dataType = getDataType(colDataType);
				tapAlterFieldAttributesEvent.dataType(ValueChange.create(dataType));
				consumer.accept(tapAlterFieldAttributesEvent);
			}
		}
	}
}

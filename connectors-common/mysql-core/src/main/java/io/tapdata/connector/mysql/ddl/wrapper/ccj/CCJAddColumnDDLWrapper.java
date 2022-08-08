package io.tapdata.connector.mysql.ddl.wrapper.ccj;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.entity.Capability;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.pdk.apis.entity.ConnectionOptions.DDL_NEW_FIELD_EVENT;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 14:24
 **/
public class CCJAddColumnDDLWrapper extends CCJBaseDDLWrapper {
	@Override
	public List<Capability> getCapabilities() {
		return Collections.singletonList(Capability.create(DDL_NEW_FIELD_EVENT).type(Capability.TYPE_DDL));
	}

	@Override
	public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) throws Throwable {
		verifyAlter(ddl);
		String tableName = getTableName(ddl);
		TapTable tapTable = null == tableMap ? null : tableMap.get(tableName);
		List<AlterExpression> alterExpressions = ddl.getAlterExpressions();
		for (AlterExpression alterExpression : alterExpressions) {
			if (alterExpression.getOperation() != AlterOperation.ADD) {
				continue;
			}
			List<AlterExpression.ColumnDataType> colDataTypeList = alterExpression.getColDataTypeList();
			if (null == colDataTypeList) {
				continue;
			}
			TapNewFieldEvent tapNewFieldEvent = new TapNewFieldEvent();
			tapNewFieldEvent.setTableId(tableName);
			for (AlterExpression.ColumnDataType columnDataType : colDataTypeList) {
				String columnName = columnDataType.getColumnName();
				columnName = removeFirstAndLastApostrophe(columnName);
				ColDataType colDataType = columnDataType.getColDataType();
				String dataType = getDataType(colDataType);
				TapField tapField = new TapField(columnName, dataType);
				List<String> columnSpecs = columnDataType.getColumnSpecs();
				String preSpec = "";
				for (String columnSpec : columnSpecs) {
					columnSpec = columnSpec.toLowerCase();
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
							tapField.nullable(!"not".equals(preSpec));
							preSpec = "";
							break;
						case "key":
						case "KEY":
							if (StringUtils.equalsAny(preSpec, "", "primary", "PRIMARY")) {
								tapField.primaryKeyPos(null != tapTable ? (tapTable.getMaxPKPos() + 1) : 1);
								preSpec = "";
							}
							break;
						case "auto_increment":
						case "AUTO_INCREMENT":
							tapField.autoInc(true);
							preSpec = "";
							break;
						default:
							if (StringUtils.equalsAnyIgnoreCase(preSpec, "default", "DEFAULT")) {
								tapField.defaultValue(removeFirstAndLastApostrophe(columnSpec));
								preSpec = "";
							} else if (StringUtils.equalsAnyIgnoreCase(preSpec, "comment", "COMMENT")) {
								tapField.comment(removeFirstAndLastApostrophe(columnSpec));
								preSpec = "";
							}
							break;
					}
				}
				setColumnPos(tapTable, tapField);
				tapNewFieldEvent.field(tapField);
				consumer.accept(tapNewFieldEvent);
			}
		}
	}
}

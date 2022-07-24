package io.tapdata.connector.mysql.ddl.wrapper.ccj;

import io.tapdata.connector.mysql.ddl.wrapper.BaseDDLWrapper;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-07-04 17:33
 **/
public abstract class CCJBaseDDLWrapper extends BaseDDLWrapper<Alter> {

	protected void verifyAlter(Alter alter) {
		if (null == alter) {
			throw new RuntimeException("DDL parser result is null");
		}
		Table table = alter.getTable();
		if (null == table) {
			throw new RuntimeException("DDL parser result's table object is null");
		}
		if (StringUtils.isBlank(table.getName())) {
			throw new RuntimeException("DDL parser result's table name is blank");
		}
	}

	protected String getTableName(Alter ddl) {
		Table table = ddl.getTable();
		String tableName = table.getName();
		if (StringUtils.isNotBlank(tableName)) {
			tableName = StringUtils.removeStart(tableName, "`");
			tableName = StringUtils.removeEnd(tableName, "`");
		}
		return tableName;
	}

	protected String getDataType(ColDataType colDataType) {
		StringBuilder dataType = new StringBuilder(colDataType.getDataType());
		List<String> argumentsStringList = colDataType.getArgumentsStringList();
		if (null != argumentsStringList && argumentsStringList.size() > 0) {
			dataType.append("(")
					.append(String.join(",", argumentsStringList))
					.append(")");
		}
		return dataType.toString();
	}

	protected void setColumnPos(TapTable tapTable, TapField tapField) {
		if (null != tapTable) {
			tapField.pos(tapTable.getMaxPos() + 1);
		} else {
			tapField.pos(1);
		}
	}

	protected String removeFirstAndLastApostrophe(String str) {
		if (StringUtils.isNotBlank(str)) {
			str = StringUtils.removeStart(str, "'");
			str = StringUtils.removeEnd(str, "'");
		}
		return str;
	}
}

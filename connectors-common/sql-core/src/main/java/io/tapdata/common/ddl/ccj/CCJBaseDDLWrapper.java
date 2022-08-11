package io.tapdata.common.ddl.ccj;

import io.tapdata.common.ddl.wrapper.BaseDDLWrapper;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-07-04 17:33
 **/
public abstract class CCJBaseDDLWrapper extends BaseDDLWrapper<Alter> {

    protected final String spilt;

    public CCJBaseDDLWrapper(String spilt) {
        this.spilt = spilt;
    }

    protected void verifyAlter(Alter alter) {
        if (null == alter) {
            throw new RuntimeException("DDL parser result is null");
        }
        Table table = alter.getTable();
        if (null == table) {
            throw new RuntimeException("DDL parser result's table object is null");
        }
        if (EmptyKit.isBlank(table.getName())) {
            throw new RuntimeException("DDL parser result's table name is blank");
        }
    }

    protected String getTableName(Alter ddl) {
        Table table = ddl.getTable();
        return StringKit.removeHeadTail(table.getName(), spilt, null);
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
}

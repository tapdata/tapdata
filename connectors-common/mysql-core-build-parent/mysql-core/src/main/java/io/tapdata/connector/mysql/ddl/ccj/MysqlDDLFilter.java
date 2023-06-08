package io.tapdata.connector.mysql.ddl.ccj;

import io.tapdata.common.ddl.DDLFilter;
import io.tapdata.common.ddl.type.DDLType;

public class MysqlDDLFilter extends DDLFilter {

    @Override
    public String filterDDL(DDLType type, String ddl) {
        switch (type.getType()) {
            case ADD_COLUMN:
            case MODIFY_COLUMN:
            case CHANGE_COLUMN:
                String lowerDdl = ddl.toLowerCase();
                if (lowerDdl.endsWith("first")) {
                    return ddl.substring(0, lowerDdl.lastIndexOf("first"));
                }
        }
        return super.filterDDL(type, ddl);
    }
}

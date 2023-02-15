package io.tapdata.connector.tidb.ddl.ccj;

import io.tapdata.common.ddl.type.DDLType;
import io.tapdata.common.ddl.type.WrapperType;

public class TidbWrapperType extends WrapperType {
    public TidbWrapperType() {
        this.ddlTypes.add(new DDLType(DDLType.Type.ADD_COLUMN, "alter\\s+table\\s+.*\\s+add(\\s+column){0,1}.+", false, "ADD [COLUMN] (col_name column_definition,...)", TidbAddColumnDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.ALTER_COLUMN, "alter\\s+table\\s+.*\\s+alter(\\s+column){0,1}.+", false, "ALTER [COLUMN] col_name column_definition", TidbAlterColumnAttrsDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.RENAME_COLUMN, "alter\\s+table\\s+.*\\s+rename\\s+column.+\\s+to\\s+.+", false, "RENAME COLUMN old_col_name TO new_col_name", TidbAlterColumnNameDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.DROP_COLUMN, "alter\\s+table\\s+.*\\s+drop(\\s+column){0,1}.+", false, "DROP [COLUMN] col_name", TidbDropColumnDDLWrapper.class));
    }
}

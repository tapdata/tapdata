package io.tapdata.connector.mysql.ddl.ccj;

import io.tapdata.common.ddl.type.DDLType;
import io.tapdata.common.ddl.type.WrapperType;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 16:33
 **/
public class MysqlWrapperType extends WrapperType {
    public MysqlWrapperType() {
        this.ddlTypes.add(new DDLType(DDLType.Type.ADD_COLUMN, "alter\\s+table\\s+.*\\s+add\\s+(column){0,1}((?!(index|constraint|fulltext|key|spatial|partition)).)+.+", false, "ADD [COLUMN] (col_name column_definition,...)", MysqlAddColumnDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.CHANGE_COLUMN, "alter\\s+table\\s+.*\\s+change\\s+(column){0,1}.+", false, "CHANGE [COLUMN] old_col_name new_col_name column_definition",
                MysqlAlterColumnNameDDLWrapper.class, MysqlAlterColumnAttrsDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.MODIFY_COLUMN, "alter\\s+table\\s+.*\\s+modify\\s+(column){0,1}.+", false, "MODIFY [COLUMN] col_name column_definition", MysqlAlterColumnAttrsDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.RENAME_COLUMN, "alter\\s+table\\s+.*\\s+rename\\s+column.+\\s+to\\s+.+", false, "RENAME COLUMN old_col_name TO new_col_name", MysqlAlterColumnNameDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.DROP_COLUMN, "alter\\s+table\\s+.*\\s+drop\\s+(column){0,1}((?!(check|constraint|index|key|primary\\skey|foreign\\skey)).)+.+", false, "DROP [COLUMN] col_name", MysqlDropColumnDDLWrapper.class));
    }
}

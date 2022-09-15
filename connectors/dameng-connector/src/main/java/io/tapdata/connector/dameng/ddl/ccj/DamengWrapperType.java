package io.tapdata.connector.dameng.ddl.ccj;

import io.tapdata.common.ddl.type.DDLType;
import io.tapdata.common.ddl.type.WrapperType;


/**
 * @author lemon
 */
public class DamengWrapperType extends WrapperType {
    public DamengWrapperType() {
        this.ddlTypes.add(new DDLType(DDLType.Type.ADD_COLUMN, "alter\\s+table\\s+.*\\s+add(\\s+column){0,1}.+", false, "ADD [COLUMN] (col_name column_definition,...)", DamengAddColumnDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.CHANGE_COLUMN, "alter\\s+table\\s+.*\\s+change(\\s+column){0,1}.+", false, "CHANGE [COLUMN] old_col_name new_col_name column_definition",
                DamengAlterColumnNameDDLWrapper.class, DamengAlterColumnAttrsDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.MODIFY_COLUMN, "alter\\s+table\\s+.*\\s+modify(\\s+column){0,1}.+", false, "MODIFY [COLUMN] col_name column_definition", DamengAlterColumnAttrsDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.RENAME_COLUMN, "alter\\s+table\\s+.*\\s+rename\\s+column.+\\s+to\\s+.+", false, "RENAME COLUMN old_col_name TO new_col_name", DamengAlterColumnNameDDLWrapper.class));
        this.ddlTypes.add(new DDLType(DDLType.Type.DROP_COLUMN, "alter\\s+table\\s+.*\\s+drop(\\s+column){0,1}.+", false, "DROP [COLUMN] col_name", DamengDropColumnDDLWrapper.class));
    }
}

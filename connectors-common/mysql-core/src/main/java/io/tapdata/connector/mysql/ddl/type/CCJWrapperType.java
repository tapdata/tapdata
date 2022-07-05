package io.tapdata.connector.mysql.ddl.type;

import io.tapdata.connector.mysql.ddl.wrapper.ccj.CCJAddColumnDDLWrapper;
import io.tapdata.connector.mysql.ddl.wrapper.ccj.CCJAlterColumnAttrsDDLWrapper;
import io.tapdata.connector.mysql.ddl.wrapper.ccj.CCJAlterColumnNameDDLWrapper;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 16:33
 **/
public class CCJWrapperType extends WrapperType {
	public CCJWrapperType() {
		this.ddlTypes.add(new DDLType(DDLType.Type.ADD_COLUMN, "alter\\s+table\\s+.*\\s+add(\\s+column){0,1}.+", false, "ADD [COLUMN] (col_name column_definition,...)", CCJAddColumnDDLWrapper.class));
		this.ddlTypes.add(new DDLType(DDLType.Type.CHANGE_COLUMN, "alter\\s+table\\s+.*\\s+change(\\s+column){0,1}.+", false, "CHANGE [COLUMN] old_col_name new_col_name column_definition",
				CCJAlterColumnNameDDLWrapper.class, CCJAlterColumnAttrsDDLWrapper.class));
		this.ddlTypes.add(new DDLType(DDLType.Type.MODIFY_COLUMN, "alter\\s+table\\s+.*\\s+modify(\\s+column){0,1}.+", false, "MODIFY [COLUMN] col_name column_definition", null));
		this.ddlTypes.add(new DDLType(DDLType.Type.RENAME_COLUMN, "alter\\s+table\\s+.*\\s+rename\\s+column.+\\s+to\\s+.+", false, "RENAME COLUMN old_col_name TO new_col_name", null));
		this.ddlTypes.add(new DDLType(DDLType.Type.DROP_COLUMN, "alter\\s+table\\s+.*\\s+drop(\\s+column){0,1}.+", false, "DROP [COLUMN] col_name", null));
	}
}

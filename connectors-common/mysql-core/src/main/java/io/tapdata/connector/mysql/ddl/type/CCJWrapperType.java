package io.tapdata.connector.mysql.ddl.type;

import io.tapdata.connector.mysql.ddl.wrapper.CCJAddColumnWrapper;

import java.util.ArrayList;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 16:33
 **/
public class CCJWrapperType extends WrapperType {
	public CCJWrapperType() {
		this.ddlTypes = new ArrayList<DDLType>(){{
			add(new DDLType("alter\\s+table\\s+.*\\s+add(\\s+column){0,1}.+", false, "ADD [COLUMN] (col_name column_definition,...)", CCJAddColumnWrapper.class));
			add(new DDLType("alter\\s+table\\s+.*\\s+change(\\s+column){0,1}.+", false, "CHANGE [COLUMN] old_col_name new_col_name column_definition", null));
			add(new DDLType("alter\\s+table\\s+.*\\s+modify(\\s+column){0,1}.+", false, "MODIFY [COLUMN] col_name column_definition", null));
			add(new DDLType("alter\\s+table\\s+.*\\s+rename\\s+column.+\\s+to\\s+.+", false, "RENAME COLUMN old_col_name TO new_col_name", null));
			add(new DDLType("alter\\s+table\\s+.*\\s+drop(\\s+column){0,1}.+", false, "DROP [COLUMN] col_name", null));
		}};
	}
}

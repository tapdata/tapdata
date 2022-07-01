package io.tapdata.connector.mysql.ddl.type;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 16:32
 **/
public abstract class WrapperType {

	protected List<DDLType> ddlTypes;

	public List<DDLType> getDdlTypes() {
		return ddlTypes;
	}
}

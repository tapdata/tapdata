package io.tapdata.connector.mariadb.ddl.type;

import java.util.ArrayList;
import java.util.List;


public abstract class WrapperType {

	protected final List<DDLType> ddlTypes;

	protected WrapperType() {
		this.ddlTypes = new ArrayList<>();
	}

	public List<DDLType> getDdlTypes() {
		return ddlTypes;
	}
}

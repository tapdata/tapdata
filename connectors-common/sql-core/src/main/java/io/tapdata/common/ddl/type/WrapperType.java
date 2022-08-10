package io.tapdata.common.ddl.type;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 16:32
 **/
public abstract class WrapperType {

	protected final List<DDLType> ddlTypes;

	protected WrapperType() {
		this.ddlTypes = new ArrayList<>();
	}

	public List<DDLType> getDdlTypes() {
		return ddlTypes;
	}
}

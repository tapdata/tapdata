package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-03-17 21:45
 **/
public class TapPdkWriteMissingPrivilegesEx extends TapMissingPrivilegesEx {

	private static final long serialVersionUID = -7609868209969768057L;

	public TapPdkWriteMissingPrivilegesEx(String pdkId, Object operation, List<String> privileges, Throwable cause) {
		super(PDKExCode_10.WRITE_MISSING_PRIVILEGES, pdkId, operation, privileges, cause);
	}

	@Override
	protected String getType() {
		return "write data";
	}
}

package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 14:48
 **/
public class TapPdkReadMissingPrivilegesEx extends TapMissingPrivilegesEx {

	private static final long serialVersionUID = 1431155464041853524L;

	public TapPdkReadMissingPrivilegesEx(String pdkId, Object operation, List<String> privileges, Throwable cause) {
		super(PDKExCode_10.READ_MISSING_PRIVILEGES, pdkId, operation, privileges, cause);
	}

	@Override
	protected String getType() {
		return "read data";
	}
}

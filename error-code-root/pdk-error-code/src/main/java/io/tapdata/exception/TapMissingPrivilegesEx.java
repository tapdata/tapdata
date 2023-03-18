package io.tapdata.exception;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-03-17 21:45
 **/
public abstract class TapMissingPrivilegesEx extends TapPdkBaseException {
	private static final long serialVersionUID = 8933194681404701875L;

	private Object operation;
	private List<String> privileges;

	protected TapMissingPrivilegesEx(String code, String pdkId, Object operation, List<String> privileges, Throwable cause) {
		super(code, pdkId, cause);
		this.operation = operation;
		this.privileges = privileges;
	}

	public Object getOperation() {
		return operation;
	}

	public List<String> getPrivileges() {
		return privileges;
	}

	@Override
	public String getMessage() {
		String prefix = "Missing privileges when doing some operation on %s. ";
		if (getClass().equals(TapPdkReadMissingPrivilegesEx.class)) {
			prefix = "Missing privileges when read data from %s. ";
		} else if (getClass().equals(TapPdkWriteMissingPrivilegesEx.class)) {
			prefix = "Missing privileges when write data to %s. ";
		}
		return String.format(prefix + "\n - Executing operation: %s\n - Missing privileges: [%s]", pdkId, operation, String.join(", ", privileges));
	}

	@Override
	protected void clone(TapRuntimeException tapRuntimeException) {
		if (tapRuntimeException instanceof TapMissingPrivilegesEx) {
			TapMissingPrivilegesEx tapMissingPrivilegesEx = (TapMissingPrivilegesEx) tapRuntimeException;
			tapMissingPrivilegesEx.operation = this.operation;
			tapMissingPrivilegesEx.privileges = new ArrayList<>(this.privileges);
		}
		super.clone(tapRuntimeException);
	}
}

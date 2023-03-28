package io.tapdata.exception;

import org.apache.commons.lang3.StringUtils;

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
	private String preferCommand;

	protected TapMissingPrivilegesEx(String code, String pdkId, Object operation, List<String> privileges, Throwable cause) {
		super(code, pdkId, cause);
		this.operation = operation;
		this.privileges = privileges;
	}

	protected TapMissingPrivilegesEx(String code, String pdkId, Object operation, List<String> privileges, String preferCommand, Throwable cause) {
		super(code, pdkId, cause);
		this.operation = operation;
		this.privileges = privileges;
		this.preferCommand = preferCommand;
	}

	public Object getOperation() {
		return operation;
	}

	public List<String> getPrivileges() {
		return privileges;
	}

	abstract protected String getType();

	@Override
	public String getMessage() {
		String message = "Missing privileges when %s on %s. \n - Executing operation: %s\n - Missing privileges: [%s]";
		String type = StringUtils.isNotBlank(getType()) ? getType() : "do some operation";
		message = String.format(message, type, pdkId, operation, String.join(", ", privileges));
		if (StringUtils.isNotBlank(preferCommand)) {
			message += "\n - Prefer command: " + preferCommand;
		}
		return message;
	}

	@Override
	protected void clone(TapRuntimeException tapRuntimeException) {
		if (tapRuntimeException instanceof TapMissingPrivilegesEx) {
			TapMissingPrivilegesEx tapMissingPrivilegesEx = (TapMissingPrivilegesEx) tapRuntimeException;
			tapMissingPrivilegesEx.operation = this.operation;
			tapMissingPrivilegesEx.privileges = new ArrayList<>(this.privileges);
			tapMissingPrivilegesEx.preferCommand = this.preferCommand;
		}
		super.clone(tapRuntimeException);
	}
}

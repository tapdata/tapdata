package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 14:48
 **/
public class TapPDKBatchReadMissingPrivilegesException extends TapPDKException {

	private static final long serialVersionUID = 1431155464041853524L;
	private final String database;
	private final Object operation;
	private final List<String> privileges;

	public TapPDKBatchReadMissingPrivilegesException(String database, Object operation, List<String> privileges, Throwable cause) {
		super(PDKExCode_10.BATCH_READ_MISSING_PRIVILEGES, cause);
		this.database = database;
		this.operation = operation;
		this.privileges = privileges;
	}

	public String getDatabase() {
		return database;
	}

	public Object getOperation() {
		return operation;
	}

	public List<String> getPrivileges() {
		return privileges;
	}

	@Override
	public String getMessage() {
		return String.format("Database %s missing privileges when performing %s operation: %s", database, operation, privileges);
	}
}

package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

/**
 * @author samuel
 * @Description
 * @create 2023-03-17 22:10
 **/
public class TapPdkViolateUniqueEx extends TapPdkBaseException {

	private static final long serialVersionUID = 5770704249978162273L;

	private String targetFieldName;
	private Object data;
	private Object constraint;

	public TapPdkViolateUniqueEx(String pdkId, String targetFieldName, Object data, Object constraint, Throwable cause) {
		super(PDKExCode_10.WRITE_VIOLATE_UNIQUE_CONSTRAINT, pdkId, cause);
		this.targetFieldName = targetFieldName;
		this.data = data;
		this.constraint = constraint;
	}

	public String getTargetFieldName() {
		return targetFieldName;
	}

	public Object getData() {
		return data;
	}

	public Object getConstraint() {
		return constraint;
	}

	@Override
	protected void clone(TapRuntimeException tapRuntimeException) {
		if (tapRuntimeException instanceof TapPdkViolateUniqueEx) {
			TapPdkViolateUniqueEx tapPdkViolateUniqueEx = (TapPdkViolateUniqueEx) tapRuntimeException;
			tapPdkViolateUniqueEx.targetFieldName = this.targetFieldName;
			tapPdkViolateUniqueEx.data = this.data;
			tapPdkViolateUniqueEx.constraint = this.constraint;
		}
		super.clone(tapRuntimeException);
	}

	@Override
	public String getMessage() {
		return String.format(
				"Unable to write data to %s due to violation of unique constraint.\n - Target field: %s\n - Data to be written(%s): %s\n - Unique constraint: %s",
				pdkId, targetFieldName, data == null ? null : data.getClass().getSimpleName(), data, constraint
		);
	}
}

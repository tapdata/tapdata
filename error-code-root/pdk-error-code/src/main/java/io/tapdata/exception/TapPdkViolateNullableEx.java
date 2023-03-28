package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

/**
 * @author samuel
 * @Description
 * @create 2023-03-17 22:18
 **/
public class TapPdkViolateNullableEx extends TapPdkBaseException {

	private static final long serialVersionUID = -660568134623080606L;

	private String targetFieldName;

	public TapPdkViolateNullableEx(String pdkId, String targetFieldName, Throwable cause) {
		super(PDKExCode_10.WRITE_VIOLATE_NULLABLE_CONSTRAINT, pdkId, cause);
		this.targetFieldName = targetFieldName;
	}

	public String getTargetFieldName() {
		return targetFieldName;
	}

	@Override
	protected void clone(TapRuntimeException tapRuntimeException) {
		if (tapRuntimeException instanceof TapPdkViolateNullableEx) {
			TapPdkViolateNullableEx tapPdkViolateNullableEx = (TapPdkViolateNullableEx) tapRuntimeException;
			tapPdkViolateNullableEx.targetFieldName = this.targetFieldName;
		}
		super.clone(tapRuntimeException);
	}

	@Override
	public String getMessage() {
		return String.format("Unable to write data to %s due to violation of nullable constraint.\n - Target field: %s", pdkId, targetFieldName);
	}
}

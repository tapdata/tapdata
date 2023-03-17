package io.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2023-03-17 22:18
 **/
public class TapPdkViolateNullableEx extends TapPdkBaseException {

	private static final long serialVersionUID = -660568134623080606L;

	private String targetFieldName;

	public TapPdkViolateNullableEx(String code, String pdkId, Throwable cause, String targetFieldName) {
		super(code, pdkId, cause);
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

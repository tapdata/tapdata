package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

/**
 * @author samuel
 * @Description
 * @create 2023-03-17 22:09
 **/
public class TapPdkWriteLengthEx extends TapPdkBaseException {

	private static final long serialVersionUID = 8963523368007626368L;

	private String targetFieldName;
	private String targetFieldType;
	private Object data;

	public TapPdkWriteLengthEx(String pdkId, String targetFieldName, String targetFieldType, Object data, Throwable cause) {
		super(PDKExCode_10.WRITE_LENGTH_INVALID, pdkId, cause);
		this.targetFieldName = targetFieldName;
		this.targetFieldType = targetFieldType;
		this.data = data;
	}

	public String getTargetFieldName() {
		return targetFieldName;
	}

	public String getTargetFieldType() {
		return targetFieldType;
	}

	public Object getData() {
		return data;
	}

	@Override
	protected void clone(TapRuntimeException tapRuntimeException) {
		if (tapRuntimeException instanceof TapPdkWriteLengthEx) {
			TapPdkWriteLengthEx tapPdkWriteLengthEx = (TapPdkWriteLengthEx) tapRuntimeException;
			tapPdkWriteLengthEx.targetFieldName = this.targetFieldName;
			tapPdkWriteLengthEx.targetFieldType = this.targetFieldType;
			tapPdkWriteLengthEx.data = this.data;
		}
		super.clone(tapRuntimeException);
	}

	@Override
	public String getMessage() {
		return String.format(
				"Target length in %s does not match the incoming data when write record.\n - Target field: %s, type: %s\n - Data to be written: %s\n - Java type: %s",
				pdkId, targetFieldName, targetFieldType,
				data, data.getClass().getName()
		);
	}
}

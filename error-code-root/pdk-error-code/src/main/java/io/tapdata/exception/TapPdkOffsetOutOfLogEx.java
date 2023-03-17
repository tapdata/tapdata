package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 15:15
 **/
public class TapPdkOffsetOutOfLogEx extends TapPdkBaseException {
	private static final long serialVersionUID = 2974491580768660945L;
	private Object offset;

	public TapPdkOffsetOutOfLogEx(String pdkId, Object offset, Throwable cause) {
		super(PDKExCode_10.OFFSET_OUT_OF_LOG, pdkId, cause);
		this.offset = offset;
	}

	public Object getOffset() {
		return offset;
	}

	@Override
	public String getMessage() {
		if (null != offset) {
			return String.format("Increment start point exceeds the log time window of %s, start point: %s", pdkId, offset);
		} else {
			return String.format("Increment start point exceeds the log time window of %s, start point is null", pdkId);
		}
	}

	@Override
	protected void clone(TapRuntimeException tapRuntimeException) {
		if (tapRuntimeException instanceof TapPdkOffsetOutOfLogEx) {
			TapPdkOffsetOutOfLogEx tapPDKOffsetOutOfLogEx = (TapPdkOffsetOutOfLogEx) tapRuntimeException;
			tapPDKOffsetOutOfLogEx.pdkId = this.pdkId;
			tapPDKOffsetOutOfLogEx.offset = this.offset;
		}
		super.clone(tapRuntimeException);
	}
}

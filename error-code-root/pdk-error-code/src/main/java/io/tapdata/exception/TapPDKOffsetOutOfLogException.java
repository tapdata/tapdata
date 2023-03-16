package io.tapdata.exception;

import io.tapdata.PDKExCode_10;
import org.apache.commons.lang3.StringUtils;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 15:15
 **/
public class TapPDKOffsetOutOfLogException extends TapPDKException {
	private static final long serialVersionUID = 2974491580768660945L;
	private final String pdkId;
	private final Object offset;

	public TapPDKOffsetOutOfLogException(String pdkId, Object offset, Throwable cause) {
		super(PDKExCode_10.OFFSET_OUT_OF_LOG, cause);
		if (StringUtils.isBlank(pdkId)) {
			throw new IllegalArgumentException(String.format("Construct %s failed: Pdk id cannot be empty", TapPDKOffsetOutOfLogException.class.getSimpleName()));
		}
		this.pdkId = pdkId;
		this.offset = offset;
	}

	public String getPdkId() {
		return pdkId;
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
}

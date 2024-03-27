package io.tapdata.exception;

import io.tapdata.error.EngineExCode_33;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/27 15:12 Create
 */
public class NotSupportRecordEventTypeException extends TapCodeException {

	private final String typeClass;

	public NotSupportRecordEventTypeException(String typeClassName) {
		super(EngineExCode_33.NOT_SUPPORT_RECORD_EVENT_TYPE_EXCEPTION, "Not support TapRecordEvent type: " + typeClassName);
		this.typeClass = typeClassName;
	}

	public String getTypeClass() {
		return typeClass;
	}
}

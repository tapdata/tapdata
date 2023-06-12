package com.tapdata.entity.inspect;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/7 2:31 下午
 * @description
 */
public enum InspectStatus {

	PAUSE("pause"),
	SCHEDULING("scheduling"),
	RUNNING("running"),
	ERROR("error"),
	DONE("done"),
	WAITING("waiting"),
	STOPPING("stopping"),
	;

	private final String code;

	InspectStatus(String code) {
		this.code = code;
	}

	public static InspectStatus get(String code) {
		for (InspectStatus status : InspectStatus.values()) {
			if (status.code.equals(code))
				return status;
		}
		return null;
	}

	public String getCode() {
		return code;
	}
}

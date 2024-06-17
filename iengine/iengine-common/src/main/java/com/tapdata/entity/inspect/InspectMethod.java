package com.tapdata.entity.inspect;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/9 1:10 上午
 * @description
 */
public enum InspectMethod {
	ROW_COUNT("row_count"),
	FIELD("field"),
	JOINTFIELD("jointField"),
	CDC_COUNT("cdcCount"),
	JUNIT_TEST("junitTest"),
	HASH("hash")
	;

	private final String code;

	InspectMethod(String code) {
		this.code = code;
	}

	private static Map<String, InspectMethod> map = new HashMap<>();

	static {
		for (InspectMethod value : InspectMethod.values()) {
			map.put(value.code, value);
		}
	}

    public String getCode() {
        return code;
    }

    public boolean equalsString(String name) {
		return this.code.equals(name);
	}

	public static InspectMethod get(String name) {
		if (StringUtils.isBlank(name)) {
			return InspectMethod.FIELD;
		}
		InspectMethod inspectMethod = map.get(name);
		if (inspectMethod != null) {
			return inspectMethod;
		}

		return FIELD;
	}
}

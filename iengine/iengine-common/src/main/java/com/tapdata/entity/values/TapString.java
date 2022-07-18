package com.tapdata.entity.values;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Dexter
 */
public class TapString extends AbstractTapValue<String> {
	public static String GET_STRING = "getString";
	public static String GET_BOOLEAN = "getBoolean";

	// Constructors

	public TapString() {
	}

	/**
	 * Accept a {@code String} value into TapString.
	 *
	 * @param origin: {@code String} Value to be accepted into TapString.
	 */
	public TapString(String origin) {
		this.setOrigin(origin);
		this.setConverter(() -> origin);
	}

	// Getters: Getting desired value from container

	public Boolean getBoolean(AbstractTapValue<String> value) throws Exception {
		if (value == null) {
			return false;
		}
		String origin = value.get();
		if (StringUtils.isBlank(origin)) {
			return false;
		}
		if (StringUtils.equalsAnyIgnoreCase(origin, "y", "1")) {
			return true;
		} else {
			return false;
		}
	}
}

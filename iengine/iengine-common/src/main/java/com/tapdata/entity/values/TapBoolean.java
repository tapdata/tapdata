package com.tapdata.entity.values;

/**
 * @author Dexter
 */
public class TapBoolean extends AbstractTapValue<Boolean> {
	public static String GET_BOOLEAN = "getBoolean";
	public static String GET_NUMBER = "getNumber";

	// Constructors

	public TapBoolean() {
	}

	/**
	 * Accept a {@link Boolean} value into TapBoolean.
	 */
	public TapBoolean(Boolean origin) {
		this.setOrigin(origin);
		this.setConverter(() -> origin);
	}

	public TapBoolean(boolean origin) {
		this((Boolean) origin);
	}

	// Getters: Getting desired value from container

	/**
	 * Convert TapBoolean container to a {@code boolean}.
	 */
	public boolean getBoolean(AbstractTapValue<?> container) throws Exception {
		return ((TapBoolean) container).get();
	}

	/**
	 * Convert TapBoolean container to a {@code number}.
	 */
	public int getNumber(AbstractTapValue<?> container) throws Exception {
		return ((TapBoolean) container).get() ? 1 : 0;
	}
}

package com.tapdata.entity.values;

import org.apache.commons.lang3.ArrayUtils;

/**
 * @author Dexter
 */
public class TapBytes extends AbstractTapValue<byte[]> {
	public static String GET_BYTES = "getBytes";

	// Constructors

	public TapBytes() {
	}

	/**
	 * Accept a {@code byte[]} array value into TapBytes.
	 */
	public TapBytes(byte[] origin) {
		this.setOrigin(origin);
		this.setConverter(() -> origin);
	}

	/**
	 * Accept a {@link Byte[]} array value into TapBytes.
	 */
	public TapBytes(Byte[] origin) {
		this.setOrigin(origin);
		this.setConverter(() -> ArrayUtils.toPrimitive(origin));
	}

	// Getters: Getting desired value from this

	/**
	 * Convert TapBytes container to a {@code byte[]}.
	 */
	public byte[] getBytes(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin() instanceof byte[]) {
			return (byte[]) container.getOrigin();
		}
		return ((TapBytes) container).get();
	}
}

package com.tapdata.entity.values;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * TapNumber is a container for numeric values, we currently support these numeric types and the according
 * constructors are also provided.
 * <ul>
 *   <li>Integer: {@code byte}, {@code short}, {@code int}, {@code long}, {@code BigInteger} etc.</li>
 *   <li>Floating Point: {@code float}, {@code double} etc.</li>
 *   <li>Decimal: {@code BigDecimal} etc.</li>
 *   <li>Number: {@code Number} etc.</li>
 * </ul>
 * <p> If you want to add some other types into this container({@code String} .e.g), please convert it to
 * the supported types first, {@code BigDecimal} is recommended. </p>
 *
 * @author Dexter
 */
public class TapNumber extends AbstractTapValue<BigDecimal> {
	public static String GET_BYTE = "getByte";
	public static String GET_SHORT = "getShort";
	public static String GET_INT = "getInt";
	public static String GET_LONG = "getLong";
	public static String GET_BIGINTEGER = "getBigInteger";
	public static String GET_FLOAT = "getFloat";
	public static String GET_DOUBLE = "getDouble";
	public static String GET_BIGDECIMAL = "getBigDecimal";

	// Constructors

	public TapNumber() {
	}

	/**
	 * Accept a {@link Byte} value into TapNumber.
	 */
	public TapNumber(Byte origin) {
		this.setOrigin(origin);
		this.setConverter(() -> new BigDecimal(origin));
	}

	public TapNumber(byte origin) {
		this((Byte) origin);
	}

	/**
	 * Accept a {@link Short} value into TapNumber.
	 */
	public TapNumber(Short origin) {
		this.setOrigin(origin);
		this.setConverter(() -> new BigDecimal(origin));
	}

	public TapNumber(short origin) {
		this((Short) origin);
	}

	/**
	 * Accept an {@link Integer} value into TapNumber.
	 */
	public TapNumber(Integer origin) {
		this.setOrigin(origin);
		this.setConverter(() -> new BigDecimal(origin));
	}

	public TapNumber(int origin) {
		this((Integer) origin);
	}

	/**
	 * Accept a long value into TapNumber.
	 */

	public TapNumber(Long origin) {
		this.setOrigin(origin);
		this.setConverter(() -> new BigDecimal(origin));
	}

	public TapNumber(long origin) {
		this((Long) origin);
	}

	/**
	 * Accept a {@link BigInteger} value into TapNumber.
	 */
	public TapNumber(BigInteger origin) {
		this.setOrigin(origin);
		this.setConverter(() -> new BigDecimal(origin));
	}

	/**
	 * Accept a {@link Float} value into TapNumber.
	 */
	public TapNumber(Float origin) {
		this.setOrigin(origin);
		this.setConverter(() -> new BigDecimal(origin));
	}

	public TapNumber(float origin) {
		this((Float) origin);
	}

	/**
	 * Accept a {@link Double} value into TapNumber.
	 */
	public TapNumber(Double origin) {
		this.setOrigin(origin);
		this.setConverter(() -> new BigDecimal(origin));
	}

	public TapNumber(double origin) {
		this((Double) origin);
	}

	/**
	 * Accept a {@link BigDecimal} value into TapNumber.
	 */
	public TapNumber(BigDecimal origin) {
		this.setOrigin(origin);
		this.setConverter(() -> origin);
	}

	/**
	 * Accept a {@link Number} value into TapNumber.
	 */
	public TapNumber(Number origin) {
		this.setOrigin(origin);
		this.setConverter(() -> new BigDecimal(origin.toString()));
	}

	// Getters: Getting desired value from container

	/**
	 * Get Byte value from the TapNumber container.
	 *
	 * @throws ArithmeticException if {@code this.originalValue} won't fit in a {@code byte}
	 */
	public byte getByte(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin().getClass() == Byte.class) {
			return (byte) container.getOrigin();
		}
		return ((TapNumber) container).get().byteValueExact();
	}

	public short getShort(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin().getClass() == Short.class) {
			return (short) container.getOrigin();
		}
		return ((TapNumber) container).get().shortValueExact();
	}

	public int getInt(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin().getClass() == Integer.class) {
			return (int) container.getOrigin();
		}
		return ((TapNumber) container).get().intValueExact();
	}

	public long getLong(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin().getClass() == Long.class) {
			return (long) container.getOrigin();
		}
		return ((TapNumber) container).get().longValueExact();
	}

	public BigInteger getBigInteger(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin().getClass() == BigInteger.class) {
			return (BigInteger) container.getOrigin();
		}
		return ((TapNumber) container).get().toBigIntegerExact();
	}

	public float getFloat(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin().getClass() == Float.class) {
			return (Float) container.getOrigin();
		}
		return ((TapNumber) container).get().floatValue();
	}

	public double getDouble(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin().getClass() == Double.class) {
			return (Double) container.getOrigin();
		}
		return ((TapNumber) container).get().doubleValue();
	}

	public BigDecimal getBigDecimal(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin().getClass() == BigDecimal.class) {
			return (BigDecimal) container.getOrigin();
		}
		return ((TapNumber) container).get();
	}

	// Does not provide get Number Type value function since you can get more specific type of
	// the Number like short, double etc.

	// Arithmetic Operations

	public TapNumber plus(byte value) {
		return this;
	}


}

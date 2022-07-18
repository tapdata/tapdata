package com.tapdata.entity.values;

import java.sql.Time;

/**
 * @author Dexter
 */
public class TapTime extends AbstractTapValue<Time> {
	public static String GET_TIME = "getTime";

	// Constructors

	public TapTime() {
	}

	/**
	 * Accept a {@link Time} value into TapTime.
	 */
	public TapTime(Time origin) {
		this.setOrigin(origin);
		this.setConverter(() -> origin);
	}

	// Getters: Getting desired value from container

	/**
	 * Get the {@link java.sql.Time} from the {@code TapTime} Tap Value container.
	 *
	 * <p> This getter is provided since JDBC use {@link java.sql.Time} to present
	 * time related column data. </p>
	 */
	public Time getTime(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin() instanceof Time) {
			return (Time) container.getOrigin();
		}
		return ((TapTime) container).get();
	}

	/**
	 * Get the {@link String} from the {@code TapTime} Tap Value container.
	 *
	 * <p> This getter is provided since some dbs do not support time related column
	 * data, so we use {@link String} to present time related column data. </p>
	 */
	@Override
	public String getString(AbstractTapValue<?> container) throws Exception {
		return ((TapTime) container).get().toString();
	}
}

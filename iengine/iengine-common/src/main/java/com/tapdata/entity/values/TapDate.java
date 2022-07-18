package com.tapdata.entity.values;

import com.tapdata.constant.DateUtil;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * This Tap Value type mainly contains three values:
 * <ul>
 *   <li>Year</li>
 *   <li>Month</li>
 *   <li>Day</li>
 * </ul>
 * <p> The converted value is the number of milliseconds since January 1, 1970,
 * 00:00:00 GMT. </p>
 *
 * @author Dexter
 */
public class TapDate extends AbstractTapValue<Long> {
	public static String GET_JAVA_SQL_DATE = "getJavaSqlDate";
	public static String GET_JAVA_SQL_TIMESTAMP = "getJavaSqlTimestamp";
	public static String GET_TIMESTAMP_MILLIS = "getTimestampMillis";

	// Constructors

	public TapDate() {
	}

	/**
	 * Accept a {@link java.util.Date} value into TapDate.
	 */
	public TapDate(java.util.Date origin) {
		this.setOrigin(origin);
		this.setConverter(origin::getTime);
	}

	/**
	 * Accept a {@link java.sql.Date} value into TapDate.
	 */
	public TapDate(java.sql.Date origin) {
		this.setOrigin(origin);
		this.setConverter(origin::getTime);
	}

	/**
	 * Accept a {@link Calendar} value into TapDate.
	 */
	public TapDate(Calendar origin) {
		this.setOrigin(origin);
		this.setConverter(origin::getTimeInMillis);
	}

	/**
	 * Accept a {@link java.sql.Timestamp} value into TapDate.
	 *
	 * <p> This constructor is provided since JDBC use {@link java.sql.Timestamp} to present
	 * DATE column data. </p>
	 */
	public TapDate(java.sql.Timestamp origin) {
		this.setOrigin(origin);
		this.setConverter(origin::getTime);
	}

	// Getters: Getting desired value from this

	/**
	 * Get the {@link java.sql.Timestamp} from the {@code TapDate} Tap Value container.
	 *
	 * <p> This getter is provided since JDBC use {@link java.sql.Timestamp} to present
	 * date related column data. </p>
	 */
	public java.sql.Timestamp getJavaSqlTimestamp(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin() instanceof java.sql.Timestamp) {
			return (java.sql.Timestamp) container.getOrigin();
		}

		Long o = (Long) container.get();
		long convertTimestamp = DateUtil.convertTimestamp(o, TimeZone.getDefault(), TimeZone.getTimeZone("GMT"));
		return new Timestamp(convertTimestamp);
	}

	/**
	 * Get the {@link java.sql.Date} from the {@code TapDate} Tap Value container.
	 *
	 * <p> This getter is provided since JDBC use {@link java.sql.Date} to present
	 * date related column data. </p>
	 */
	public java.sql.Date getJavaSqlDate(AbstractTapValue<?> container) throws Exception {
		if (container.getOrigin() instanceof java.sql.Date) {
			return (java.sql.Date) container.getOrigin();
		}

		return new java.sql.Date(((TapDate) container).get());
	}

	/**
	 * Get milliseconds since the epoch from the {@code TapDate} Tap Value container.
	 */
	public Long getTimestampMillis(AbstractTapValue<?> container) throws Exception {
		return ((TapDate) container).get();
	}

	@Override
	public String getString(AbstractTapValue<?> container) throws Exception {
		return new Date(((TapDate) container).get()).toString();
	}
}

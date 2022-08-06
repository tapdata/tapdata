package com.tapdata.entity.values;

import com.tapdata.constant.DateUtil;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.TimeZone;

/**
 * @author Dexter
 */
public class TapDatetime extends AbstractTapValue<Long> {
	public static String GET_JAVA_SQL_TIMESTAMP = "getJavaSqlTimestamp";
	public static String GET_TIMESTAMP_Second = "getTimestampSecond";
	public static String GET_TIMESTAMP_MILLIS = "getTimestampMillis";

	// Constructors

	public TapDatetime() {
	}

	/**
	 * Accept a {@link Integer} value into TapeDateTime.
	 *
	 * <p> The long value presents the standard timestamp value in second. </p>
	 */
	public TapDatetime(Integer origin) {
		this.setOrigin(origin);
		this.setConverter(() -> origin * 1_000_000L);
	}

	public TapDatetime(int origin) {
		this((Integer) origin);
	}

	/**
	 * Accept a {@link Long} value into TapeDatetime.
	 *
	 * <p> The long value presents the standard timestamp value in microsecond. </p>
	 */
	public TapDatetime(Long origin) {
		this.setOrigin(origin);
		this.setConverter(() -> origin);
	}

	public TapDatetime(long origin) {
		this((Long) origin);
	}

	/**
	 * Accept a {@link java.sql.Timestamp} value into TapDatetime.
	 *
	 * <p> This constructor is provided since JDBC use {@link java.sql.Timestamp} to present
	 * DATETIME column data. </p>
	 */
	public TapDatetime(Timestamp origin) {
		this.setOrigin(origin);
		this.setConverter(() -> {
			ZoneId customZoneId = getContext().getJobSourceConn().getCustomZoneId();
			if (customZoneId != null) {
				return origin.toLocalDateTime().atZone(customZoneId).toInstant().toEpochMilli();
			} else {
				return origin.toLocalDateTime().atZone(ZoneId.of("GMT")).toInstant().toEpochMilli();
			}
		});
	}

	// Getters: Getting the desired value from container

	/**
	 * Get the {@link java.sql.Timestamp} from the {@code TapDatetime} Tap Value container.
	 *
	 * <p> This getter is provided since JDBC use {@link java.sql.Timestamp} to present
	 * date related column data. </p>
	 */
	public java.sql.Timestamp getJavaSqlTimestamp(AbstractTapValue<?> container) throws Exception {
		Instant instant = Instant.ofEpochMilli(((TapDatetime) container).get());
		ZoneId zoneId = container.getContext().getJobTargetConn().getCustomZoneId();
		if (zoneId != null) {
			return DateUtil.instant2Timestamp(instant, zoneId);
		}
		Long o = (Long) container.get();
		long convertTimestamp = DateUtil.convertTimestamp(o, TimeZone.getDefault(), TimeZone.getTimeZone("GMT"));
		return new Timestamp(convertTimestamp);
	}

	/**
	 * Get milliseconds since the epoch from the {@code TapDate} Tap Value container.
	 */
	public Long getTimestampMillis(AbstractTapValue<?> container) throws Exception {
		return ((TapDatetime) container).get();
	}

	@Override
	public String getString(AbstractTapValue<?> container) throws Exception {
		return new Timestamp(((TapDatetime) container).get()).toString();
	}

}

package io.tapdata.base;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.exception.DatetimeFormatException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

/**
 * @author samuel
 * @Description
 * @create 2023-10-24 14:56
 **/
class ConnectorBaseTest {
	private final DateTime dateTime = new DateTime(LocalDateTime.of(2024, 10, 24, 13, 33, 25, 101203001));

	@Test
	void testFormatTapDateTime() {
		String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'";
		String formatTapDateTime = ConnectorBase.formatTapDateTime(dateTime, pattern);
		Assertions.assertEquals("2024-10-24T13:33:25.101203001Z", formatTapDateTime);
	}

	@Test
	void testFormatTapDateTimeNullDateTime() {
		String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'";
		Assertions.assertThrows(IllegalArgumentException.class, () -> ConnectorBase.formatTapDateTime(null, pattern));
	}

	@Test
	void testFormatTapDateTimeBlankFormatPattern() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> ConnectorBase.formatTapDateTime(dateTime, null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> ConnectorBase.formatTapDateTime(dateTime, ""));
		Assertions.assertThrows(IllegalArgumentException.class, () -> ConnectorBase.formatTapDateTime(dateTime, "  "));
	}

	@Test
	void testFormatTapDateTimeWrongFormatPattern() {
		Assertions.assertThrows(DatetimeFormatException.class, () -> ConnectorBase.formatTapDateTime(dateTime, "aaaaa"));
	}
}

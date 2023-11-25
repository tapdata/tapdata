package io.tapdata.flow.engine.V2.util;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2023-11-24 18:18
 **/
class TapEventUtilTest {
	@Test
	@DisplayName("GetTableId method main process test")
	void testGetTableId() {
		TapBaseEvent tapBaseEvent = mock(TapBaseEvent.class);
		when(tapBaseEvent.getTableId()).thenReturn("test_table");
		String actual = TapEventUtil.getTableId(tapBaseEvent);
		assertEquals("test_table", actual);
	}

	@Test
	@DisplayName("GetTableId method when not TapBaseEvent")
	void testGetTableIdWhenNotTapBaseEvent() {
		TapEvent tapEvent = mock(TapEvent.class);
		String actual = TapEventUtil.getTableId(tapEvent);
		assertEquals("", actual);
	}
}

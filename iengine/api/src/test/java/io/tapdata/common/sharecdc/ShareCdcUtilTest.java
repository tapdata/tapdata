package io.tapdata.common.sharecdc;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-05-16 22:20
 **/
class ShareCdcUtilTest {
	@Nested
	class iterateAndHandleSpecialTypeTest {

		private Map<String, Object> map;
		private Function<Object, Object> function;
		private IllegalDatePredicate illegalDatePredicate;

		@BeforeEach
		void setUp() {
			map = new HashMap<>();
			map.put("_str", "test");
			map.put("_int", 111);
			Map<String, Object> subMap = new HashMap<>();
			subMap.put("_str", "stest");
			subMap.put("_int", 222);
			map.put("_map", subMap);
			List<Object> list = new ArrayList<>();
			list.add("a");
			list.add("b");
			map.put("_list", list);
			function = mock(Function.class);
			when(function.apply(any())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
			illegalDatePredicate = mock(IllegalDatePredicate.class);
		}

		@Test
		void testMainProcess() {
			assertDoesNotThrow(() -> ShareCdcUtil.iterateAndHandleSpecialType(map, function));
			verify(function, times(4)).apply(any());
		}

		@Test
		void testIllegalDate() {
			when(illegalDatePredicate.test("test")).thenReturn(true);
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().init();
			assertDoesNotThrow(() -> ShareCdcUtil.iterateAndHandleSpecialType(map, function, illegalDatePredicate, tapInsertRecordEvent, EventType.AFTER));
			assertTrue(tapInsertRecordEvent.getContainsIllegalDate());
			assertFalse(tapInsertRecordEvent.getAfterIllegalDateFieldName().isEmpty());
			assertEquals(1, tapInsertRecordEvent.getAfterIllegalDateFieldName().size());
			assertEquals("_str", tapInsertRecordEvent.getAfterIllegalDateFieldName().get(0));

			TapUpdateRecordEvent tapUpdateRecordEvent = TapUpdateRecordEvent.create().init();
			assertDoesNotThrow(() -> ShareCdcUtil.iterateAndHandleSpecialType(map, function, illegalDatePredicate, tapUpdateRecordEvent, EventType.BEFORE));
			assertTrue(tapUpdateRecordEvent.getContainsIllegalDate());
			assertFalse(tapUpdateRecordEvent.getBeforeIllegalDateFieldName().isEmpty());
			assertEquals(1, tapUpdateRecordEvent.getBeforeIllegalDateFieldName().size());
			assertEquals("_str", tapUpdateRecordEvent.getBeforeIllegalDateFieldName().get(0));
		}
	}

}
package io.tapdata.flow.engine.V2.util;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
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

	@Test
	void testGetRemoveFields() {
		TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
		List<String> stringList = new ArrayList<>();
		stringList.add("abc");
		stringList.add("123");
		tapUpdateRecordEvent.setRemovedFields(stringList);
		List<String> removeFields = TapEventUtil.getRemoveFields(tapUpdateRecordEvent);
		assertEquals("abc", removeFields.get(0));
		assertEquals("123", removeFields.get(1));
	}

	@Test
	void testGetNullRemoveFields() {
		TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
		List<String> removeFields = TapEventUtil.getRemoveFields(tapUpdateRecordEvent);
		assertNull(removeFields);
	}

	@Test
	void testNullEvent() {
		List<String> removeFields = TapEventUtil.getRemoveFields(null);
		assertNull(removeFields);
	}

	@Test
	@DisplayName("get TapUpdateEvent IsReplaceEvent is true")
	void test1() {
		TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
		tapUpdateRecordEvent.setIsReplaceEvent(true);
		Boolean isReplaceEvent = TapEventUtil.getIsReplaceEvent(tapUpdateRecordEvent);
		assertEquals(Boolean.TRUE, isReplaceEvent);
	}

	@Test
	@DisplayName("get TapUpdateEvent IsReplaceEvent is fales")
	void test2() {
		TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
		tapUpdateRecordEvent.setIsReplaceEvent(false);
		Boolean isReplaceEvent = TapEventUtil.getIsReplaceEvent(tapUpdateRecordEvent);
		assertEquals(Boolean.FALSE, isReplaceEvent);
	}

	@Test
	@DisplayName("in addition to TapUpdateEvent get IsReplaceEvent")
	void test3() {
		TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
		Boolean isReplaceEvent = TapEventUtil.getIsReplaceEvent(tapInsertRecordEvent);
		assertEquals(Boolean.FALSE, isReplaceEvent);
	}

	@Nested
	class getIllegalField {
		@Test
		@DisplayName("test getIllegalField method for TapInsertRecordEvent")
		void test1() {
			TapEvent tapEvent = new TapInsertRecordEvent();
			List mock = mock(List.class);
			((TapInsertRecordEvent) tapEvent).setAfterIllegalDateFieldName(mock);
			Map<String, List<String>> actual = TapEventUtil.getIllegalField(tapEvent);
			assertEquals(mock, actual.get("after"));
		}

		@Test
		@DisplayName("test getIllegalField method for TapUpdateRecordEvent")
		void test2() {
			TapEvent tapEvent = new TapUpdateRecordEvent();
			List before = mock(List.class);
			List after = mock(List.class);
			((TapUpdateRecordEvent) tapEvent).setBeforeIllegalDateFieldName(before);
			((TapUpdateRecordEvent) tapEvent).setAfterIllegalDateFieldName(after);
			Map<String, List<String>> actual = TapEventUtil.getIllegalField(tapEvent);
			assertEquals(before, actual.get("before"));
			assertEquals(after, actual.get("after"));
		}

		@Test
		@DisplayName("test getIllegalField method for TapDeleteRecordEvent")
		void test3() {
			TapEvent tapEvent = new TapDeleteRecordEvent();
			List mock = mock(List.class);
			((TapDeleteRecordEvent) tapEvent).setBeforeIllegalDateFieldName(mock);
			Map<String, List<String>> actual = TapEventUtil.getIllegalField(tapEvent);
			assertEquals(mock, actual.get("before"));
		}

		@Test
		@DisplayName("test getIllegalField method for other event")
		void test4() {
			TapEvent tapEvent = new HeartbeatEvent();
			Map<String, List<String>> actual = TapEventUtil.getIllegalField(tapEvent);
			assertNull(actual);
		}
	}

	@Nested
	class setContainsIllegalDate {
		private TapEvent event;
		private boolean containsIllegalDate;

		@Test
		@DisplayName("test setContainsIllegalDate method for TapRecordEvent")
		void test1() {
			event = new TapInsertRecordEvent();
			containsIllegalDate = true;
			TapEventUtil.setContainsIllegalDate(event, containsIllegalDate);
			assertEquals(containsIllegalDate, ((TapInsertRecordEvent) event).getContainsIllegalDate());
		}

		@Test
		@DisplayName("test setContainsIllegalDate method for TapAlterFieldNameEvent")
		void test2() {
			event = new TapAlterFieldNameEvent();
			containsIllegalDate = true;
			TapEventUtil.setContainsIllegalDate(event, containsIllegalDate);
			assertNotNull(event);
		}
	}

	@Nested
	class addBeforeIllegalDateField {
		private TapEvent tapEvent;
		private String fieldName;

		@Test
		@DisplayName("test addBeforeIllegalDateField method for TapUpdateRecordEvent")
		void test1() {
			tapEvent = new TapUpdateRecordEvent();
			fieldName = "test_field";
			TapEventUtil.addBeforeIllegalDateField(tapEvent, fieldName);
			assertEquals(fieldName, ((TapUpdateRecordEvent) tapEvent).getBeforeIllegalDateFieldName().get(0));
			assertNull(((TapUpdateRecordEvent) tapEvent).getAfterIllegalDateFieldName());
		}

		@Test
		@DisplayName("test addBeforeIllegalDateField method for TapUpdateRecordEvent when fieldNameList is not null")
		void test2() {
			tapEvent = new TapUpdateRecordEvent();
			List<String> fieldNameList = new ArrayList<>();
			fieldNameList.add("field");
			((TapUpdateRecordEvent) tapEvent).setBeforeIllegalDateFieldName(fieldNameList);
			fieldName = "test_field";
			TapEventUtil.addBeforeIllegalDateField(tapEvent, fieldName);
			assertEquals(fieldNameList, ((TapUpdateRecordEvent) tapEvent).getBeforeIllegalDateFieldName());
			assertEquals(2, ((TapUpdateRecordEvent) tapEvent).getBeforeIllegalDateFieldName().size());
			assertNull(((TapUpdateRecordEvent) tapEvent).getAfterIllegalDateFieldName());
		}

		@Test
		@DisplayName("test addBeforeIllegalDateField method for TapDeleteRecordEvent")
		void test3() {
			tapEvent = new TapDeleteRecordEvent();
			fieldName = "test_field";
			TapEventUtil.addBeforeIllegalDateField(tapEvent, fieldName);
			assertEquals(fieldName, ((TapDeleteRecordEvent) tapEvent).getBeforeIllegalDateFieldName().get(0));
		}

		@Test
		@DisplayName("test addBeforeIllegalDateField method for TapDeleteRecordEvent when fieldNameList is not null")
		void test4() {
			tapEvent = new TapDeleteRecordEvent();
			List<String> fieldNameList = new ArrayList<>();
			fieldNameList.add("field");
			((TapDeleteRecordEvent) tapEvent).setBeforeIllegalDateFieldName(fieldNameList);
			fieldName = "test_field";
			TapEventUtil.addBeforeIllegalDateField(tapEvent, fieldName);
			assertEquals(fieldNameList, ((TapDeleteRecordEvent) tapEvent).getBeforeIllegalDateFieldName());
			assertEquals(2, ((TapDeleteRecordEvent) tapEvent).getBeforeIllegalDateFieldName().size());
		}

		@Test
		@DisplayName("test addBeforeIllegalDateField method for TapInsertRecordEvent")
		void test5() {
			tapEvent = new TapInsertRecordEvent();
			TapEventUtil.addBeforeIllegalDateField(tapEvent, "test_field");
			assertNotNull(tapEvent);
		}
	}

	@Nested
	class addAfterIllegalDateField {
		private TapEvent tapEvent;
		private String fieldName;

		@Test
		@DisplayName("test addAfterIllegalDateField method for TapInsertRecordEvent")
		void test1() {
			tapEvent = new TapInsertRecordEvent();
			fieldName = "test_field";
			TapEventUtil.addAfterIllegalDateField(tapEvent, fieldName);
			assertEquals(fieldName, ((TapInsertRecordEvent) tapEvent).getAfterIllegalDateFieldName().get(0));
		}

		@Test
		@DisplayName("test addBeforeIllegalDateField method for TapInsertRecordEvent when fieldNameList is not null")
		void test2() {
			tapEvent = new TapInsertRecordEvent();
			List<String> fieldNameList = new ArrayList<>();
			fieldNameList.add("field");
			((TapInsertRecordEvent) tapEvent).setAfterIllegalDateFieldName(fieldNameList);
			fieldName = "test_field";
			TapEventUtil.addAfterIllegalDateField(tapEvent, fieldName);
			assertEquals(fieldNameList, ((TapInsertRecordEvent) tapEvent).getAfterIllegalDateFieldName());
			assertEquals(2, ((TapInsertRecordEvent) tapEvent).getAfterIllegalDateFieldName().size());
		}

		@Test
		@DisplayName("test addBeforeIllegalDateField method for TapUpdateRecordEvent")
		void test3() {
			tapEvent = new TapUpdateRecordEvent();
			fieldName = "test_field";
			TapEventUtil.addAfterIllegalDateField(tapEvent, fieldName);
			assertEquals(fieldName, ((TapUpdateRecordEvent) tapEvent).getAfterIllegalDateFieldName().get(0));
			assertNull(((TapUpdateRecordEvent) tapEvent).getBeforeIllegalDateFieldName());
		}

		@Test
		@DisplayName("test addBeforeIllegalDateField method for TapUpdateRecordEvent when fieldNameList is not null")
		void test4() {
			tapEvent = new TapUpdateRecordEvent();
			List<String> fieldNameList = new ArrayList<>();
			fieldNameList.add("field");
			((TapUpdateRecordEvent) tapEvent).setAfterIllegalDateFieldName(fieldNameList);
			fieldName = "test_field";
			TapEventUtil.addAfterIllegalDateField(tapEvent, fieldName);
			assertEquals(fieldNameList, ((TapUpdateRecordEvent) tapEvent).getAfterIllegalDateFieldName());
			assertEquals(2, ((TapUpdateRecordEvent) tapEvent).getAfterIllegalDateFieldName().size());
			assertNull(((TapUpdateRecordEvent) tapEvent).getBeforeIllegalDateFieldName());
		}

		@Test
		@DisplayName("test addBeforeIllegalDateField method for TapDeleteRecordEvent")
		void test5() {
			tapEvent = new TapDeleteRecordEvent();
			TapEventUtil.addAfterIllegalDateField(tapEvent, "test_field");
			assertNotNull(tapEvent);
		}
	}

	@Nested
	class SwapTableIdAndMasterTableIdTest {
		@Test
		void testNotTapBaseEvent() {
			TapEvent tapEvent = mock(TapEvent.class);
			Assertions.assertDoesNotThrow(() -> TapEventUtil.swapTableIdAndMasterTableId(tapEvent));
		}

		@Test
		void testPartitionMasterTableIdIsEmpty() {
			TapBaseEvent tapEvent = new TapInsertRecordEvent();
			tapEvent.setTableId("id");
			tapEvent.setPartitionMasterTableId(null);
			Assertions.assertDoesNotThrow(() -> TapEventUtil.swapTableIdAndMasterTableId(tapEvent));
			Assertions.assertEquals("id", tapEvent.getPartitionMasterTableId());
			Assertions.assertEquals("id", tapEvent.getTableId());
		}
		@Test
		void testPartitionMasterTableIdNotEmpty() {
			TapBaseEvent tapEvent = new TapInsertRecordEvent();
			tapEvent.setTableId("id");
			tapEvent.setPartitionMasterTableId("masterId");
			Assertions.assertDoesNotThrow(() -> TapEventUtil.swapTableIdAndMasterTableId(tapEvent));
			Assertions.assertEquals("id", tapEvent.getPartitionMasterTableId());
			Assertions.assertEquals("masterId", tapEvent.getTableId());
		}
	}

	@Nested
	@DisplayName("Method setRemoveFields test")
	class setRemoveFieldsTest {
		@Test
		@DisplayName("test insert event")
		void test1() {
			TapEvent tapEvent = new TapInsertRecordEvent();
			List<String> removeFields = new ArrayList<>();
			removeFields.add("test");
			TapEventUtil.setRemoveFields(tapEvent, removeFields);
			assertEquals(removeFields, ((TapInsertRecordEvent) tapEvent).getRemovedFields());
		}

		@Test
		@DisplayName("test update event")
		void test2() {
			TapEvent tapEvent = new TapUpdateRecordEvent();
			List<String> removeFields = new ArrayList<>();
			removeFields.add("test");
			TapEventUtil.setRemoveFields(tapEvent, removeFields);
			assertEquals(removeFields, ((TapUpdateRecordEvent) tapEvent).getRemovedFields());
		}

		@Test
		@DisplayName("test delete event")
		void test3() {
			TapEvent tapEvent = new TapDeleteRecordEvent();
			List<String> removeFields = new ArrayList<>();
			removeFields.add("test");
			assertDoesNotThrow(() -> TapEventUtil.setRemoveFields(tapEvent, removeFields));
		}

		@Test
		@DisplayName("test event is null")
		void test4() {
			List<String> removeFields = new ArrayList<>();
			assertDoesNotThrow(() -> TapEventUtil.setRemoveFields(null, removeFields));
		}
	}
}

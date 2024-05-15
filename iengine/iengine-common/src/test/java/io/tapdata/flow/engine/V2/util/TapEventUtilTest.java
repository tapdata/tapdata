package io.tapdata.flow.engine.V2.util;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
	void testGetRemoveFields(){
		TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
		List<String> stringList=new ArrayList<>();
		stringList.add("abc");
		stringList.add("123");
		tapUpdateRecordEvent.setRemovedFields(stringList);
		List<String> removeFields = TapEventUtil.getRemoveFields(tapUpdateRecordEvent);
		assertEquals("abc",removeFields.get(0));
		assertEquals("123",removeFields.get(1));
	}
	@Test
	void testGetNullRemoveFields(){
		TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
		List<String> removeFields = TapEventUtil.getRemoveFields(tapUpdateRecordEvent);
		assertNull(removeFields);
	}
	@Test
	void testNullEvent(){
		List<String> removeFields = TapEventUtil.getRemoveFields(null);
		assertNull(removeFields);
	}

	@Test
	@DisplayName("get TapUpdateEvent IsReplaceEvent is true")
	void test1(){
		TapUpdateRecordEvent tapUpdateRecordEvent=new TapUpdateRecordEvent();
		tapUpdateRecordEvent.setIsReplaceEvent(true);
		Boolean isReplaceEvent = TapEventUtil.getIsReplaceEvent(tapUpdateRecordEvent);
		assertEquals(Boolean.TRUE,isReplaceEvent);
	}
	@Test
	@DisplayName("get TapUpdateEvent IsReplaceEvent is fales")
	void test2(){
		TapUpdateRecordEvent tapUpdateRecordEvent=new TapUpdateRecordEvent();
		tapUpdateRecordEvent.setIsReplaceEvent(false);
		Boolean isReplaceEvent = TapEventUtil.getIsReplaceEvent(tapUpdateRecordEvent);
		assertEquals(Boolean.FALSE,isReplaceEvent);
	}
	@Test
	@DisplayName("in addition to TapUpdateEvent get IsReplaceEvent")
	void test3(){
		TapInsertRecordEvent tapInsertRecordEvent=new TapInsertRecordEvent();
		Boolean isReplaceEvent = TapEventUtil.getIsReplaceEvent(tapInsertRecordEvent);
		assertEquals(Boolean.FALSE,isReplaceEvent);
	}
	@Nested
	class getIllegalField{
		@Test
		@DisplayName("test getIllegalField method for TapInsertRecordEvent")
		void test1(){
			TapEvent tapEvent = new TapInsertRecordEvent();
			List mock = mock(List.class);
			((TapInsertRecordEvent)tapEvent).setAfterIllegalDateFieldName(mock);
			Map<String, List<String>> actual = TapEventUtil.getIllegalField(tapEvent);
			assertEquals(mock, actual.get("after"));
		}
		@Test
		@DisplayName("test getIllegalField method for TapUpdateRecordEvent")
		void test2(){
			TapEvent tapEvent = new TapUpdateRecordEvent();
			List before = mock(List.class);
			List after = mock(List.class);
			((TapUpdateRecordEvent)tapEvent).setBeforeIllegalDateFieldName(before);
			((TapUpdateRecordEvent)tapEvent).setAfterIllegalDateFieldName(after);
			Map<String, List<String>> actual = TapEventUtil.getIllegalField(tapEvent);
			assertEquals(before, actual.get("before"));
			assertEquals(after, actual.get("after"));
		}
		@Test
		@DisplayName("test getIllegalField method for TapDeleteRecordEvent")
		void test3(){
			TapEvent tapEvent = new TapDeleteRecordEvent();
			List mock = mock(List.class);
			((TapDeleteRecordEvent)tapEvent).setBeforeIllegalDateFieldName(mock);
			Map<String, List<String>> actual = TapEventUtil.getIllegalField(tapEvent);
			assertEquals(mock, actual.get("before"));
		}
		@Test
		@DisplayName("test getIllegalField method for other event")
		void test4(){
			TapEvent tapEvent = new HeartbeatEvent();
			Map<String, List<String>> actual = TapEventUtil.getIllegalField(tapEvent);
			assertNull(actual);
		}
	}
}

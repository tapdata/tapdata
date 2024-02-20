package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.pdk.core.api.ConnectorNode;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("HazelcastSourcePdkDataNode Class Test")
public class HazelcastSourcePdkDataNodeTest extends BaseHazelcastNodeTest {

	private HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		hazelcastSourcePdkDataNode = spy(new HazelcastSourcePdkDataNode(dataProcessorContext));
	}

	@Nested
	@DisplayName("flushPollingCDCOffset input TapEvent list method test")
	class flushPollingCDCOffsetTapEventListTest {
		@BeforeEach
		void setUp() {
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).flushPollingCDCOffset(any(TapEvent.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("main process test")
		void testMainProcess() {
			List<TapEvent> tapEvents = new ArrayList<>();
			tapEvents.add(new TapInsertRecordEvent().after(new HashMap<>()));
			tapEvents.add(new TapUpdateRecordEvent().after(new HashMap<>()));
			tapEvents.add(new TapDropFieldEvent().fieldName("test"));
			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapEvents);
			verify(hazelcastSourcePdkDataNode, times(1)).flushPollingCDCOffset(any(TapEvent.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("input null or empty list test")
		void testInputNullOrEmptyList() {
			hazelcastSourcePdkDataNode.getClass().getDeclaredMethod("flushPollingCDCOffset", List.class).invoke(hazelcastSourcePdkDataNode, new Object[]{null});
			verify(hazelcastSourcePdkDataNode, times(0)).flushPollingCDCOffset(any(TapEvent.class));

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(new ArrayList<>());
			verify(hazelcastSourcePdkDataNode, times(0)).flushPollingCDCOffset(any(TapEvent.class));
		}
	}

	@Nested
	@DisplayName("flushPollingCDCOffset input TapEvent method test")
	class flushPollingCDCOffsetTapEventTest {

		private SyncProgress syncProgress;
		private Map<String, Object> streamOffsetObj;
		private Map<String, Object> data;
		private TapCodecsFilterManager tapCodecsFilterManager;

		@BeforeEach
		void setUp() {
			syncProgress = new SyncProgress();
			streamOffsetObj = new HashMap<>();
			syncProgress.setStreamOffsetObj(streamOffsetObj);
			List<String> conditionFields = new ArrayList<>();
			conditionFields.add("id");
			conditionFields.add("code");
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "syncProgress", syncProgress);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "conditionFields", conditionFields);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			tapCodecsFilterManager = mock(TapCodecsFilterManager.class);
			when(connectorNode.getCodecsFilterManager()).thenReturn(tapCodecsFilterManager);
			doReturn(connectorNode).when(hazelcastSourcePdkDataNode).getConnectorNode();
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).toTapValue(any(Map.class), anyString(), eq(tapCodecsFilterManager));
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).fromTapValue(any(Map.class), eq(tapCodecsFilterManager), anyString());

			data = new HashMap<>();
			data.put("id", 1);
			data.put("code", "a");
			data.put("test", "test");
		}

		@Test
		@DisplayName("test input TapInsertEvent")
		void testTapInsertEvent() {
			doReturn(true).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapInsertRecordEvent tapInsertRecordEvent = spy(new TapInsertRecordEvent().after(data).table("dummy_test"));

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapInsertRecordEvent);

			verify(tapInsertRecordEvent, times(1)).getAfter();
			assertTrue(streamOffsetObj.containsKey("dummy_test"));
			Object tableOffsetObj = streamOffsetObj.get("dummy_test");
			assertInstanceOf(Map.class, tableOffsetObj);
			assertEquals(1, ((Map) tableOffsetObj).get("id"));
			assertEquals("a", ((Map) tableOffsetObj).get("code"));
			verify(hazelcastSourcePdkDataNode, times(1)).toTapValue(any(Map.class), anyString(), eq(tapCodecsFilterManager));
			verify(hazelcastSourcePdkDataNode, times(1)).fromTapValue(any(Map.class), eq(tapCodecsFilterManager), anyString());
		}

		@Test
		@DisplayName("test input TapUpdateEvent")
		void testTapUpdateEvent() {
			doReturn(true).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapUpdateRecordEvent tapUpdateRecordEvent = spy(new TapUpdateRecordEvent().after(data).table("dummy_test"));

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapUpdateRecordEvent);

			verify(tapUpdateRecordEvent, times(1)).getAfter();
			assertTrue(streamOffsetObj.containsKey("dummy_test"));
			Object tableOffsetObj = streamOffsetObj.get("dummy_test");
			assertInstanceOf(Map.class, tableOffsetObj);
			assertEquals(1, ((Map) tableOffsetObj).get("id"));
			assertEquals("a", ((Map) tableOffsetObj).get("code"));
			verify(hazelcastSourcePdkDataNode, times(1)).toTapValue(any(Map.class), anyString(), eq(tapCodecsFilterManager));
			verify(hazelcastSourcePdkDataNode, times(1)).fromTapValue(any(Map.class), eq(tapCodecsFilterManager), anyString());
		}

		@Test
		@DisplayName("test input TapDeleteEvent")
		void testTapDeleteEvent() {
			doReturn(true).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapDeleteRecordEvent tapDeleteRecordEvent = spy(new TapDeleteRecordEvent().before(data).table("dummy_test"));

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapDeleteRecordEvent);

			verify(tapDeleteRecordEvent, times(1)).getBefore();
			assertTrue(streamOffsetObj.containsKey("dummy_test"));
			Object tableOffsetObj = streamOffsetObj.get("dummy_test");
			assertInstanceOf(Map.class, tableOffsetObj);
			assertEquals(1, ((Map) tableOffsetObj).get("id"));
			assertEquals("a", ((Map) tableOffsetObj).get("code"));
			verify(hazelcastSourcePdkDataNode, times(1)).toTapValue(any(Map.class), anyString(), eq(tapCodecsFilterManager));
			verify(hazelcastSourcePdkDataNode, times(1)).fromTapValue(any(Map.class), eq(tapCodecsFilterManager), anyString());
		}

		@Test
		@DisplayName("test input TapDDLEvent")
		void testTapDDLEvent() {
			doReturn(true).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapDDLEvent tapDDLEvent = spy(new TapAlterFieldNameEvent());

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapDDLEvent);

			verify(hazelcastSourcePdkDataNode, times(1)).getNode();
			verify(hazelcastSourcePdkDataNode, times(0)).toTapValue(any(Map.class), anyString(), any(TapCodecsFilterManager.class));
		}

		@Test
		@DisplayName("test isPollingCDC is false")
		void testIsNotPollingCDC() {
			doReturn(false).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapInsertRecordEvent tapInsertRecordEvent = spy(new TapInsertRecordEvent());

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapInsertRecordEvent);

			verify(hazelcastSourcePdkDataNode, times(1)).getNode();
			verify(tapInsertRecordEvent, times(0)).getAfter();
		}
	}
}

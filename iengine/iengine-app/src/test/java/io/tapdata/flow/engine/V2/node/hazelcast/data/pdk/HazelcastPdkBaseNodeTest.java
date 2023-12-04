package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.TapTableMap;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-22 10:28
 **/
@DisplayName("HazelcastPdkBaseNode CLass Test")
class HazelcastPdkBaseNodeTest extends BaseHazelcastNodeTest {
	HazelcastPdkBaseNode hazelcastPdkBaseNode;
	TapCodecsFilterManager tapCodecsFilterManager;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		hazelcastPdkBaseNode = new HazelcastPdkBaseNode(dataProcessorContext) {
		};
		tapCodecsFilterManager = mock(TapCodecsFilterManager.class);
		doNothing().when(tapCodecsFilterManager).transformToTapValueMap(anyMap(), any(LinkedHashMap.class));
		when(tapCodecsFilterManager.transformFromTapValueMap(anyMap())).thenReturn(null);
	}

	@Nested
	class FunctionRetryQueryTest {
		long timestamp;
		@BeforeEach
		void init() {
			hazelcastPdkBaseNode = mock(HazelcastPdkBaseNode.class);
			timestamp = System.currentTimeMillis();
			when(hazelcastPdkBaseNode.functionRetryQuery(anyLong(), anyBoolean())).thenCallRealMethod();
		}

		/**signFunction*/
		@Test
		void functionRetryQueryNormal() {
			Update update = hazelcastPdkBaseNode.functionRetryQuery(timestamp, true);
			Document document = assertResult(update);
			Assertions.assertTrue(document.containsKey("taskRetryStartTime"));
			Assertions.assertEquals(timestamp, document.get("taskRetryStartTime"));
		}

		/**clearFunction*/
		@Test
		void functionRetryQueryNotSingle() {
			Update update = hazelcastPdkBaseNode.functionRetryQuery(timestamp, false);
			Document document = assertResult(update);
			Assertions.assertTrue(document.containsKey("functionRetryEx"));
			Assertions.assertEquals(timestamp + 5 * 60 * 1000L, document.get("functionRetryEx"));
			Assertions.assertTrue(document.containsKey("taskRetryStartTime"));
			Assertions.assertEquals(0, document.get("taskRetryStartTime"));
		}

		Document assertResult(Update update) {
			Assertions.assertNotNull(update);
			Document document = update.getUpdateObject();
			Assertions.assertNotNull(document);
			Assertions.assertTrue(document.containsKey("$set"));
			Object set = document.get("$set");
			Assertions.assertEquals(Document.class, set.getClass());
			Document setMap = (Document) set;
			Assertions.assertTrue(setMap.containsKey("functionRetryStatus"));
			Assertions.assertEquals(TaskDto.RETRY_STATUS_RUNNING, setMap.get("functionRetryStatus"));
			return setMap;
		}
	}

	@Test
	@DisplayName("DoInit method test")
	void testDoInit() {
		AtomicReference<String> log = new AtomicReference<>();
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).debug(anyString());
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).info(anyString());
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).warn(anyString());
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).error(anyString());
		doAnswer(invocationOnMock -> {
			log.set(invocationOnMock.getArgument(0));
			return null;
		}).when(mockObsLogger).fatal(anyString());
		ReflectionTestUtils.setField(hazelcastPdkBaseNode, "obsLogger", mockObsLogger);
		hazelcastPdkBaseNode.doInit(jetContext);
		Object actualObj = ReflectionTestUtils.getField(hazelcastPdkBaseNode, "logListener");
		assertNotNull(actualObj);
		assertTrue(actualObj instanceof TapLogger.LogListener);
		((TapLogger.LogListener) actualObj).debug("debug test");
		assertEquals("debug test", log.get());
		((TapLogger.LogListener) actualObj).info("info test");
		assertEquals("info test", log.get());
		((TapLogger.LogListener) actualObj).warn("warn test");
		assertEquals("warn test", log.get());
		((TapLogger.LogListener) actualObj).error("error test");
		assertEquals("error test", log.get());
		((TapLogger.LogListener) actualObj).fatal("fatal test");
		assertEquals("fatal test", log.get());
		((TapLogger.LogListener) actualObj).memory("memory test");
		assertEquals("memory test", log.get());
	}

	@Nested
	class ToTapValueTest {
		Map<String, Object> data;
		String tableName;
		TapTable tapTable;
		TapTableMap<String, TapTable> tableMap;
		LinkedHashMap<String, TapField> nameFieldMap;
		@BeforeEach
		void init() {
			hazelcastPdkBaseNode = mock(HazelcastPdkBaseNode.class);
			data = mock(HashMap.class);
			when(data.isEmpty()).thenReturn(false);
			tableName = "mock-table-name";
			tableMap = mock(TapTableMap.class);
			tapTable = mock(TapTable.class);
			nameFieldMap = mock(LinkedHashMap.class);
			when(tapTable.getNameFieldMap()).thenReturn(nameFieldMap);
			when(tableMap.get(tableName)).thenReturn(tapTable);

			dataProcessorContext = mock(DataProcessorContext.class);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tableMap);
			when(hazelcastPdkBaseNode.getDataProcessorContext()).thenReturn(dataProcessorContext);
			doCallRealMethod().when(hazelcastPdkBaseNode).toTapValue(data, tableName, tapCodecsFilterManager);
			when(hazelcastPdkBaseNode.getIsomorphism()).thenReturn(false);
		}

		@Test
		void testToTapValueNotIsomorphism() {
			assertVerify(1, data, tapCodecsFilterManager);
		}

		@Test
		void testToTapValueIsomorphism() {
			when(hazelcastPdkBaseNode.getIsomorphism()).thenReturn(true);
			assertVerify(0, data, tapCodecsFilterManager);
		}

		@Test
		void testToTapValueEmptyDataMap() {
			when(data.isEmpty()).thenReturn(true);
			assertVerify(0, data, tapCodecsFilterManager);
		}

		@Test
		void testToTapValueNullDataMap() {
			doCallRealMethod().when(hazelcastPdkBaseNode).toTapValue(null, tableName, tapCodecsFilterManager);
			assertVerify(0, null, tapCodecsFilterManager);
		}

		void assertVerify(int execTimes, Map<String, Object> dataTemp, TapCodecsFilterManager manager) {
			hazelcastPdkBaseNode.toTapValue(dataTemp, tableName, manager);
			verify(hazelcastPdkBaseNode, times(1)).getIsomorphism();
			verify(dataProcessorContext, times(execTimes)).getTapTableMap();
			verify(tableMap, times(execTimes)).get(tableName);
			verify(tapTable, times(execTimes)).getNameFieldMap();
			verify(tapCodecsFilterManager, times(execTimes)).transformToTapValueMap(data, nameFieldMap);
		}
	}

	@Nested
	class FromTapValueTest {
		Map<String, Object> data;
		@BeforeEach
		void init() {
			hazelcastPdkBaseNode = mock(HazelcastPdkBaseNode.class);
			data = mock(HashMap.class);
			when(data.isEmpty()).thenReturn(false);

			dataProcessorContext = mock(DataProcessorContext.class);
			when(hazelcastPdkBaseNode.getDataProcessorContext()).thenReturn(dataProcessorContext);
			when(hazelcastPdkBaseNode.getIsomorphism()).thenReturn(false);
			doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(data, tapCodecsFilterManager);
		}

		@Test
		void testToTapValueNotIsomorphism() {
			assertVerify(1, data, tapCodecsFilterManager);
		}

		@Test
		void testToTapValueIsomorphism() {
			when(hazelcastPdkBaseNode.getIsomorphism()).thenReturn(true);
			assertVerify(0, data, tapCodecsFilterManager);
		}

		@Test
		void testToTapValueEmptyDataMap() {
			when(data.isEmpty()).thenReturn(true);
			assertVerify(0, data, tapCodecsFilterManager);
		}

		@Test
		void testToTapValueNullDataMap() {
			doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(null, tapCodecsFilterManager);
			assertVerify(0, null, tapCodecsFilterManager);
		}

		@Test
		void testToTapValueNullTapCodecsFilterManager() {
			doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(data, null);
			assertVerify(0,  data, null);
		}

		void assertVerify(int execTimes, Map<String, Object> dataTemp, TapCodecsFilterManager manager) {
			hazelcastPdkBaseNode.fromTapValue(dataTemp, manager);
			verify(hazelcastPdkBaseNode, times(1)).getIsomorphism();
			verify(tapCodecsFilterManager, times(execTimes)).transformFromTapValueMap(data);
		}
	}

	@Nested
	class ToTapValueByCodecsTest {
		ConnectorNode node;
		TapCodecsFilterManager manager;
		TapEvent tapEvent;
		@BeforeEach
		void init() {
			tapEvent = mock(TapEvent.class);
			hazelcastPdkBaseNode = mock(HazelcastPdkBaseNode.class);
			node = mock(ConnectorNode.class);
			manager = mock(TapCodecsFilterManager.class);

			when(hazelcastPdkBaseNode.getIsomorphism()).thenReturn(false);
			when(hazelcastPdkBaseNode.getConnectorNode()).thenReturn(node);
			when(node.getCodecsFilterManager()).thenReturn(manager);
			doNothing().when(hazelcastPdkBaseNode).tapRecordToTapValue(any(TapEvent.class), any(TapCodecsFilterManager.class));
			doCallRealMethod().when(hazelcastPdkBaseNode).toTapValueByCodecs(any(TapEvent.class));
		}
		@Test
		void testToTapValueByCodecsNotIsomorphism() {
			assertVerify(1);
		}
		@Test
		void testToTapValueByCodecsIsomorphism() {
			when(hazelcastPdkBaseNode.getIsomorphism()).thenReturn(true);
			assertVerify(0);
		}
		@Test
		void testToTapValueByCodecsNullTapEvent() {
			doCallRealMethod().when(hazelcastPdkBaseNode).toTapValueByCodecs(null);
			tapEvent = null;
			assertVerify(0);
		}
		void assertVerify(int execTimes) {
			hazelcastPdkBaseNode.toTapValueByCodecs(tapEvent);
			verify(hazelcastPdkBaseNode, times(1)).getIsomorphism();
			verify(hazelcastPdkBaseNode, times(execTimes)).getConnectorNode();
			verify(node, times(execTimes)).getCodecsFilterManager();
			verify(hazelcastPdkBaseNode, times(execTimes)).tapRecordToTapValue(any(TapEvent.class), any(TapCodecsFilterManager.class));
		}
	}
}

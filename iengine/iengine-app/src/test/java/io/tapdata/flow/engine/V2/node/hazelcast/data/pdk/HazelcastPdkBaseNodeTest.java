package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.filter.TapRecordSkipDetector;
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

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		hazelcastPdkBaseNode = new HazelcastPdkBaseNode(dataProcessorContext) {
		};
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
			Assertions.assertTrue(document.containsKey("taskRetryStartTimeFlag"));
			Assertions.assertEquals(0, document.get("taskRetryStartTimeFlag"));
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
	class ToTapValueOrFromTapValueTest{
		Map<String, Object> data;
		LinkedHashMap<String, TapField> fields;
		TapCodecsFilterManager tapCodecsFilterManager;
		TapRecordSkipDetector skipDetector;
		String lastTableName;
		@BeforeEach
		void init() {
			lastTableName = "LastTableName";
			skipDetector = mock(TapRecordSkipDetector.class);
			tapCodecsFilterManager = mock(TapCodecsFilterManager.class);

			fields = mock(LinkedHashMap.class);
			hazelcastPdkBaseNode = mock(HazelcastPdkBaseNode.class);
			data = mock(HashMap.class);
			when(data.isEmpty()).thenReturn(false);

			when(hazelcastPdkBaseNode.getTableFiledMap(anyString())).thenReturn(fields);
			when(hazelcastPdkBaseNode.getLastTableName()).thenReturn(lastTableName);
			when(hazelcastPdkBaseNode.getSkipDetector()).thenReturn(skipDetector);
		}

		@Nested
		class ToTapValueTest {
			@BeforeEach
			void init() {
				doNothing().when(tapCodecsFilterManager).transformToTapValueMap(data, fields, skipDetector);
				doNothing().when(tapCodecsFilterManager).transformToTapValueMap(null, fields, skipDetector);

				doCallRealMethod().when(hazelcastPdkBaseNode).toTapValue(data, lastTableName, tapCodecsFilterManager);
			}

			@Test
			void testToTapValueNormal() {
				assertVerify(1, data, tapCodecsFilterManager);
			}

			@Test
			void testToTapValueNullTapCodecsFilterManager() {
				doCallRealMethod().when(hazelcastPdkBaseNode).toTapValue(data, lastTableName, null);
				assertVerify(0, data, null);
			}

			@Test
			void testToTapValueNullTableName() {
				doCallRealMethod().when(hazelcastPdkBaseNode).toTapValue(data, null, tapCodecsFilterManager);
				lastTableName = null;
				assertVerify(0, data, tapCodecsFilterManager);
			}

			@Test
			void testToTapValueEmptyDataMap() {
				when(data.isEmpty()).thenReturn(true);
				assertVerify(0, data, tapCodecsFilterManager);
			}

			@Test
			void testToTapValueNullDataMap() {
				doCallRealMethod().when(hazelcastPdkBaseNode).toTapValue(null, lastTableName, tapCodecsFilterManager);
				assertVerify(0, null, tapCodecsFilterManager);
			}

			void assertVerify(int execTimes, Map<String, Object> dataTemp, TapCodecsFilterManager manager) {
				hazelcastPdkBaseNode.toTapValue(dataTemp, lastTableName, manager);
				verify(hazelcastPdkBaseNode, times(execTimes)).getSkipDetector();
				verify(hazelcastPdkBaseNode, times(execTimes)).getTableFiledMap(anyString());
				verify(tapCodecsFilterManager, times(execTimes)).transformToTapValueMap(data, fields, skipDetector);
			}
		}

		@Nested
		class FromTapValueTest {
			String targetTableName;
			@BeforeEach
			void init() {
				targetTableName = "targetTableName";
				when(tapCodecsFilterManager.transformFromTapValueMap(data, fields, skipDetector)).thenReturn(null);
				when(tapCodecsFilterManager.transformFromTapValueMap(null, fields, skipDetector)).thenReturn(null);
				doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(data, tapCodecsFilterManager, targetTableName);
				doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(data, tapCodecsFilterManager, null);
				doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(data, null, targetTableName);
				doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(null, tapCodecsFilterManager, targetTableName);
			}

			@Test
			void testToTapValueNormal() {
				assertVerify(1, data, tapCodecsFilterManager);
			}

			@Test
			void testToTapValueEmptyDataMap() {
				when(data.isEmpty()).thenReturn(true);
				assertVerify(0, data, tapCodecsFilterManager);
			}

			@Test
			void testToTapValueNullDataMap() {
				doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(null, tapCodecsFilterManager, targetTableName);
				assertVerify(0,  null, tapCodecsFilterManager);
			}

			@Test
			void testToTapValueNullTapCodecsFilterManager() {
				doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(data, null, targetTableName);
				assertVerify(0, data, null);
			}

			@Test
			void testToTapValueNullTableName() {
				targetTableName = null;
				assertVerify(0, data, tapCodecsFilterManager);
			}

			void assertVerify(int execTimes, Map<String, Object> dataTemp, TapCodecsFilterManager manager) {
				hazelcastPdkBaseNode.fromTapValue(dataTemp, manager, targetTableName);
				verify(hazelcastPdkBaseNode, times(execTimes)).getSkipDetector();
				verify(hazelcastPdkBaseNode, times(execTimes)).getTableFiledMap(anyString());
				verify(tapCodecsFilterManager, times(execTimes)).transformFromTapValueMap(data, fields, skipDetector);
			}
		}
	}

	@Nested
	class GetTableFiledMapTest {
		String tableName;
		TapTable tapTable;
		TapTableMap<String, TapTable> tableMap;
		LinkedHashMap<String, TapField> nameFieldMap;

		@BeforeEach
		void init() {
			hazelcastPdkBaseNode = mock(HazelcastPdkBaseNode.class);
			tableName = "mock-table-name";
			tableMap = mock(TapTableMap.class);
			tapTable = mock(TapTable.class);
			nameFieldMap = mock(LinkedHashMap.class);

			when(hazelcastPdkBaseNode.getDataProcessorContext()).thenReturn(dataProcessorContext);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tableMap);
			when(tableMap.get(tableName)).thenReturn(tapTable);
			when(tapTable.getNameFieldMap()).thenReturn(nameFieldMap);

			when(hazelcastPdkBaseNode.getTableFiledMap(anyString())).thenCallRealMethod();
			when(hazelcastPdkBaseNode.getTableFiledMap(null)).thenCallRealMethod();
		}

		@Test
		void testGetTableFiledMap() {
			Assertions.assertEquals(nameFieldMap,
					assertVerify(tableName, 1, 1, 1, 1));
		}

		@Test
		void testGetTableFiledMapNullTableName() {
			Assertions.assertNotEquals(nameFieldMap,
					assertVerify(null, 0, 0, 0, 0));
		}

		@Test
		void testGetTableFiledMapNullDataProcessorContext() {
			when(hazelcastPdkBaseNode.getDataProcessorContext()).thenReturn(null);
			Assertions.assertNotEquals(nameFieldMap,
					assertVerify(tableName, 1, 0, 0, 0));
		}

		@Test
		void testGetTableFiledMapNullTableMap() {
			when(dataProcessorContext.getTapTableMap()).thenReturn(null);
			Assertions.assertNotEquals(nameFieldMap,
					assertVerify(tableName, 1, 1, 0, 0));
		}

		@Test
		void testGetTableFiledMapNullTapTable() {
			when(tableMap.get(tableName)).thenReturn(null);
			Assertions.assertNotEquals(nameFieldMap,
					assertVerify(tableName, 1, 1, 1, 0));
		}

		@Test
		void testGetTableFiledMapNullNameFieldMap() {
			when(tapTable.getNameFieldMap()).thenReturn(null);
			Assertions.assertNotEquals(nameFieldMap,
					assertVerify(tableName, 1, 1, 1, 1));
		}


		LinkedHashMap<String, TapField> assertVerify(String tableNameTemp,
															 int getDataProcessorContextTimes,
															 int getTapTableMapTimes,
															 int getTimes,
															 int getNameFieldMapTimes) {
			LinkedHashMap<String, TapField> tableFiledMap = hazelcastPdkBaseNode.getTableFiledMap(tableNameTemp);
			verify(hazelcastPdkBaseNode, times(getDataProcessorContextTimes)).getDataProcessorContext();
			verify(dataProcessorContext, times(getTapTableMapTimes)).getTapTableMap();
			verify(tableMap, times(getTimes)).get(tableName);
			verify(tapTable, times(getNameFieldMapTimes)).getNameFieldMap();
			return tableFiledMap;
		}
	}

}

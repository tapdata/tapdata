package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import cn.hutool.core.collection.ConcurrentHashSet;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.DmlPolicyEnum;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.PDKExCode_10;
import io.tapdata.aspect.taskmilestones.RetryLifeCycleAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.filter.TapRecordSkipDetector;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.error.TapPdkRunnerUnknownException;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.RetryLifeCycle;
import io.tapdata.schema.TapTableMap;
import io.tapdata.supervisor.TaskNodeInfo;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-22 10:28
 **/
@DisplayName("HazelcastPdkBaseNode Class Test")
class HazelcastPdkBaseNodeTest extends BaseHazelcastNodeTest {
	HazelcastPdkBaseNode hazelcastPdkBaseNode;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		hazelcastPdkBaseNode = new HazelcastPdkBaseNode(dataProcessorContext) {
		};
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
//		((TapLogger.LogListener) actualObj).info("info test");
//		assertEquals("info test", log.get());
		((TapLogger.LogListener) actualObj).warn("warn test");
		assertEquals("warn test", log.get());
		((TapLogger.LogListener) actualObj).error("error test");
		assertEquals("error test", log.get());
		((TapLogger.LogListener) actualObj).fatal("fatal test");
		assertEquals("fatal test", log.get());
//		((TapLogger.LogListener) actualObj).memory("memory test");
//		assertEquals("memory test", log.get());
	}

	@Nested
	class ToTapValueOrFromTapValueTest {
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
				doCallRealMethod().when(hazelcastPdkBaseNode).fromTapValue(data, tapCodecsFilterManager, "");
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
				assertVerify(0, null, tapCodecsFilterManager);
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

	@Nested
	@DisplayName("doClose method test")
	class doCloseTest {

		private HazelcastPdkBaseNode spyHazelcastPdkBaseNode;

		@BeforeEach
		void setUp() {
			spyHazelcastPdkBaseNode = spy(hazelcastPdkBaseNode);
			ReflectionTestUtils.setField(spyHazelcastPdkBaseNode, "obsLogger", mockObsLogger);
		}

		@Test
		@DisplayName("test pdk state map should be reset")
		void testPdkStateMapReset() {
			PdkStateMap pdkStateMap = mock(PdkStateMap.class);
			ReflectionTestUtils.setField(spyHazelcastPdkBaseNode, "pdkStateMap", pdkStateMap);
			spyHazelcastPdkBaseNode.doClose();
			verify(pdkStateMap, times(1)).reset();
		}

		@Test
		@DisplayName("test pdk state map is null when do close")
		void testPdkStateMapIsNull() {
			assertDoesNotThrow(() -> spyHazelcastPdkBaseNode.doClose());
		}
	}
	@Nested
	class SignAndCleanFuctionRetryTest{
		private HazelcastPdkBaseNode spyhazelcastPdkBaseNode;
		private HttpClientMongoOperator mongoCollection;
		@BeforeEach
		public void setUp(){
			HazelcastPdkBaseNode mockHazelcastPdkBaseNode = new HazelcastPdkBaseNode(dataProcessorContext) {
			};
			spyhazelcastPdkBaseNode=spy(mockHazelcastPdkBaseNode);
			mongoCollection=mock(HttpClientMongoOperator.class);
			ReflectionTestUtils.setField(spyhazelcastPdkBaseNode,"clientMongoOperator",mongoCollection);
		}

		@Test
		void cleanFunctionRetryTest(){
			when(mongoCollection.update(any(), any(), anyString())).thenAnswer(a -> {
				Update update = (Update)a.getArgument(1);
				Document updateObject = (Document)update.getUpdateObject().get("$set");
				String functionRetryStatus = (String) updateObject.get("functionRetryStatus");
				Integer taskRetryStartTime = (Integer) updateObject.get("taskRetryStartTime");
				assertEquals("",functionRetryStatus);
				assertEquals(0,taskRetryStartTime);
				return null;
			});
			spyhazelcastPdkBaseNode.cleanFunctionRetry("65aa211475a5ac694df51c69");
		}

		@Test
		void signFunctionRetryTest() {
			when(mongoCollection.update(any(), any(), anyString())).thenAnswer(a -> {
				Query query = (Query) a.getArgument(0);
				assertEquals(true, query.getQueryObject().containsKey("_id"));
				Document orCriteria = (Document) query.getQueryObject().get("$or");
				Document existDocument = (Document) orCriteria.get("functionRetryStatus");
				assertEquals(true, existDocument.get("$exists"));
				assertEquals(TaskDto.RETRY_STATUS_NONE, orCriteria.get("functionRetryStatus"));
				Update update = (Update) a.getArgument(1);
				Document updateObject = (Document) update.getUpdateObject().get("$set");
				String functionRetryStatus = (String) updateObject.get("functionRetryStatus");
				assertEquals("Retrying", functionRetryStatus);
				return null;
			});
			spyhazelcastPdkBaseNode.signFunctionRetry("65aa211475a5ac694df51c69");
		}
	}
	@Nested
	class CreatePdkMethodInvokerTest{
		private HazelcastPdkBaseNode spyhazelcastPdkBaseNode;
		@BeforeEach
		void setUp(){
			HazelcastPdkBaseNode hazelcastPdkBaseNode = new HazelcastPdkBaseNode(dataProcessorContext) {
			};
			spyhazelcastPdkBaseNode=spy(hazelcastPdkBaseNode);
		}
		@DisplayName("MaxRetryTimeMinute Greater than 0")
		@Test
		void test1(){
			taskDto.setId(new ObjectId("65aa211475a5ac694df51123"));
			TaskRetryConfig taskRetryConfig = TaskRetryConfig.create().maxRetryTimeSecond(900L).retryIntervalSecond(60L);
			TaskConfig taskConfig = TaskConfig.create().taskDto(taskDto).taskRetryConfig(taskRetryConfig);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);
			PDKMethodInvoker pdkMethodInvoker = spyhazelcastPdkBaseNode.createPdkMethodInvoker();
			assertEquals(60L,pdkMethodInvoker.getRetryPeriodSeconds());
			assertEquals(15L,pdkMethodInvoker.getMaxRetryTimeMinute());
		}
		@DisplayName("MaxRetryTimeMinute Less Than 0")
		@Test
		void test2(){
			TaskRetryConfig taskRetryConfig = TaskRetryConfig.create().maxRetryTimeSecond(-900L).retryIntervalSecond(60L);
			taskDto.setId(new ObjectId("65aa211475a5ac694df51c69"));
			TaskConfig taskConfig = TaskConfig.create().taskDto(taskDto).taskRetryConfig(taskRetryConfig);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);
			PDKMethodInvoker pdkMethodInvoker = spyhazelcastPdkBaseNode.createPdkMethodInvoker();
			assertEquals(0L,pdkMethodInvoker.getMaxRetryTimeMinute());
			assertEquals(60L,pdkMethodInvoker.getRetryPeriodSeconds());
		}
		@DisplayName("retryIntervalSecond less than 0")
		@Test
		void test3(){
			taskDto.setId(new ObjectId("65aa220c75a5ac694df51d04"));
			TaskRetryConfig taskRetryConfig = TaskRetryConfig.create().maxRetryTimeSecond(900L).retryIntervalSecond(-60L);
			TaskConfig taskConfig = TaskConfig.create().taskDto(taskDto).taskRetryConfig(taskRetryConfig);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);
			PDKMethodInvoker pdkMethodInvoker = spyhazelcastPdkBaseNode.createPdkMethodInvoker();
			assertEquals(60L,pdkMethodInvoker.getRetryPeriodSeconds());
		}
	}
	@Nested
	class InitDmlPolicyTest{
		HazelcastPdkBaseNode hazelcastPdkBaseNode;
		@BeforeEach
		void setUp(){
			hazelcastPdkBaseNode = mock(HazelcastTargetPdkBaseNode.class);
		}

		@Test
		void testDefault1() {
			ConnectorCapabilities connectorCapabilities = mock(ConnectorCapabilities.class);
			doCallRealMethod().when(hazelcastPdkBaseNode).initDmlPolicy(null, connectorCapabilities);
			hazelcastPdkBaseNode.initDmlPolicy(null, connectorCapabilities);
			verify(connectorCapabilities,times(1)).alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS);
			verify(connectorCapabilities,times(1)).alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS);
			verify(connectorCapabilities,times(1)).alternative(ConnectionOptions.DML_DELETE_POLICY, ConnectionOptions.DML_DELETE_POLICY_IGNORE_ON_NON_EXISTS);
		}

		@Test
		void testDefault2() {
			ConnectorCapabilities connectorCapabilities = mock(ConnectorCapabilities.class);
			TableNode tableNode = new TableNode();
			doCallRealMethod().when(hazelcastPdkBaseNode).initDmlPolicy(tableNode, connectorCapabilities);
			hazelcastPdkBaseNode.initDmlPolicy(tableNode, connectorCapabilities);
			verify(connectorCapabilities,times(1)).alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS);
			verify(connectorCapabilities,times(1)).alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS);
			verify(connectorCapabilities,times(1)).alternative(ConnectionOptions.DML_DELETE_POLICY, ConnectionOptions.DML_DELETE_POLICY_IGNORE_ON_NON_EXISTS);
		}

		@Test
		void testChangePolicy() {
			ConnectorCapabilities connectorCapabilities = mock(ConnectorCapabilities.class);
			TableNode tableNode = new TableNode();
			DmlPolicy dmlPolicy = new DmlPolicy();
			dmlPolicy.setInsertPolicy(DmlPolicyEnum.just_insert);
			dmlPolicy.setUpdatePolicy(DmlPolicyEnum.insert_on_nonexists);
			dmlPolicy.setDeletePolicy(DmlPolicyEnum.log_on_nonexists);
			tableNode.setDmlPolicy(dmlPolicy);
			doCallRealMethod().when(hazelcastPdkBaseNode).initDmlPolicy(tableNode, connectorCapabilities);
			hazelcastPdkBaseNode.initDmlPolicy(tableNode, connectorCapabilities);
			verify(connectorCapabilities,times(1)).alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_JUST_INSERT);
			verify(connectorCapabilities,times(1)).alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS);
			verify(connectorCapabilities,times(1)).alternative(ConnectionOptions.DML_DELETE_POLICY, ConnectionOptions.DML_DELETE_POLICY_IGNORE_LOG_ON_NON_EXISTS);
		}

	}

	@Nested
	@DisplayName("Method generateNodeConfig test")
	class GenerateNodeConfigTest {

		@BeforeEach
		void setUp(){
			hazelcastPdkBaseNode = mock(HazelcastPdkBaseNode.class);
			when(hazelcastPdkBaseNode.generateNodeConfig(any(Node.class), any(TaskDto.class))).thenCallRealMethod();
		}

		@Test
		@DisplayName("test TableNode")
		void testTableNode() {
			Map<String, Object> nodeConfig = TapSimplify.map(TapSimplify.entry("key", "value"));
			TableNode node1 = new TableNode();
			node1.setNodeConfig(null);
			TableNode node2 = new TableNode();
			node2.setNodeConfig(nodeConfig);
			taskDto.setDoubleActive(true);
			Assertions.assertEquals(3, hazelcastPdkBaseNode.generateNodeConfig(node1, taskDto).size());
			Assertions.assertTrue(hazelcastPdkBaseNode.generateNodeConfig(node1, taskDto).containsKey("doubleActive"));
			Assertions.assertEquals(4, hazelcastPdkBaseNode.generateNodeConfig(node2, taskDto).size());
		}

		@Test
		@DisplayName("test DatabaseNode")
		void testDatabaseNode() {
			Map<String, Object> nodeConfig = TapSimplify.map(TapSimplify.entry("key", "value"));
			DatabaseNode node1 = new DatabaseNode();
			node1.setNodeConfig(null);
			DatabaseNode node2 = new DatabaseNode();
			node2.setNodeConfig(nodeConfig);
			taskDto.setDoubleActive(true);
			Assertions.assertEquals(3, hazelcastPdkBaseNode.generateNodeConfig(node1, taskDto).size());
			Assertions.assertTrue(hazelcastPdkBaseNode.generateNodeConfig(node1, taskDto).containsKey("doubleActive"));
			Assertions.assertEquals(4, hazelcastPdkBaseNode.generateNodeConfig(node2, taskDto).size());
		}

		@Test
		@DisplayName("test LogCollectorNode")
		void testLogCollectorNode() {
			Map<String, Object> nodeConfig = TapSimplify.map(TapSimplify.entry("key", "value"));
			LogCollectorNode node1 = new LogCollectorNode();
			node1.setNodeConfig(null);
			LogCollectorNode node2 = new LogCollectorNode();
			node2.setNodeConfig(nodeConfig);
			taskDto.setDoubleActive(true);
			Assertions.assertEquals(2, hazelcastPdkBaseNode.generateNodeConfig(node1, taskDto).size());
			Assertions.assertTrue(hazelcastPdkBaseNode.generateNodeConfig(node1, taskDto).containsKey("doubleActive"));
			Assertions.assertEquals(3, hazelcastPdkBaseNode.generateNodeConfig(node2, taskDto).size());
		}

		@Test
		@DisplayName("test other node type")
		void testOtherNode() {
			CacheNode node = new CacheNode();
			taskDto.setDoubleActive(true);
			Assertions.assertEquals(2, hazelcastPdkBaseNode.generateNodeConfig(node, taskDto).size());
			Assertions.assertTrue(hazelcastPdkBaseNode.generateNodeConfig(node, taskDto).containsKey("doubleActive"));
		}

		@Test
		@DisplayName("test oldVersionTimezone")
		void testOldVersionTimezone() {
			TableNode node = new TableNode();
			taskDto.setOldVersionTimezone(true);
			Map<String, Object> nodeConfig = hazelcastPdkBaseNode.generateNodeConfig(node, taskDto);
			assertTrue(nodeConfig.containsKey(HazelcastPdkBaseNode.OLD_VERSION_TIMEZONE));
			assertTrue((Boolean) nodeConfig.get(HazelcastPdkBaseNode.OLD_VERSION_TIMEZONE));
			taskDto.setOldVersionTimezone(false);
			nodeConfig = hazelcastPdkBaseNode.generateNodeConfig(node, taskDto);
			assertTrue(nodeConfig.containsKey(HazelcastPdkBaseNode.OLD_VERSION_TIMEZONE));
			assertFalse((Boolean) nodeConfig.get(HazelcastPdkBaseNode.OLD_VERSION_TIMEZONE));
		}

		@Test
		@DisplayName("test oldVersionTimezone when set system property")
		void testOldVersionTimezone1() {
			TableNode node = new TableNode();
			taskDto.setOldVersionTimezone(true);
			System.setProperty(HazelcastPdkBaseNode.OLD_VERSION_TIME_ZONE_PROP_KEY, "false");
			Map<String, Object> nodeConfig = hazelcastPdkBaseNode.generateNodeConfig(node, taskDto);
			assertTrue(nodeConfig.containsKey(HazelcastPdkBaseNode.OLD_VERSION_TIMEZONE));
			assertFalse((Boolean) nodeConfig.get(HazelcastPdkBaseNode.OLD_VERSION_TIMEZONE));
		}
	}
	@Nested
	class throwTapCodeExceptionTest{
		@DisplayName("test throwTapCodeException when tapException")
		@Test
		void test(){
			hazelcastPdkBaseNode=mock(HazelcastPdkBaseNode.class);
			TapCodeException engineTapCodeException = new TapCodeException(TaskProcessorExCode_11.CREATE_PROCESSOR_FAILED);
			doCallRealMethod().when(hazelcastPdkBaseNode).throwTapCodeException(any(),any());
			TapCodeException tapCodeException = new TapCodeException(PDKExCode_10.OFFSET_OUT_OF_LOG);
			TapCodeException returnTapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastPdkBaseNode.throwTapCodeException(tapCodeException, engineTapCodeException);
			});
			assertEquals(returnTapCodeException.getCode(),PDKExCode_10.OFFSET_OUT_OF_LOG);
		}
		@DisplayName("test throwTapCodeException when TapPdkRunnerUnknownException ")
		@Test
		void test2(){
			hazelcastPdkBaseNode=mock(HazelcastPdkBaseNode.class);
			TapCodeException engineTapCodeException = new TapCodeException(TaskProcessorExCode_11.CREATE_PROCESSOR_FAILED);
			doCallRealMethod().when(hazelcastPdkBaseNode).throwTapCodeException(any(),any());
			TapPdkRunnerUnknownException tapCodeException=new TapPdkRunnerUnknownException(new RuntimeException("writeFaild"));
			TapCodeException returnTapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastPdkBaseNode.throwTapCodeException(tapCodeException, engineTapCodeException);
			});
			assertEquals(TaskProcessorExCode_11.CREATE_PROCESSOR_FAILED,returnTapCodeException.getCode());
		}
		@DisplayName("test throwTapCodeException when RuntimeException")
		@Test
		void test3(){
			hazelcastPdkBaseNode=mock(HazelcastPdkBaseNode.class);
			TapCodeException engineTapCodeException = new TapCodeException(TaskProcessorExCode_11.CREATE_PROCESSOR_FAILED);
			doCallRealMethod().when(hazelcastPdkBaseNode).throwTapCodeException(any(),any());
			RuntimeException runtimeException=new RuntimeException("run failed");
			TapCodeException returnTapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastPdkBaseNode.throwTapCodeException(runtimeException, engineTapCodeException);
			});
			assertEquals(TaskProcessorExCode_11.CREATE_PROCESSOR_FAILED,returnTapCodeException.getCode());
		}
	}
	@Nested
	class testGetLeakedOrThreadGroupClass{
		@DisplayName("test ")
		@Test
		void test1(){
			String nodeName = "leakNode";
			Node leakNode = mock(Node.class);
			when(leakNode.getId()).thenReturn(nodeName);
			TaskNodeInfo taskNodeInfo = new TaskNodeInfo();
			taskNodeInfo.setNode(leakNode);
			taskNodeInfo.setHasLeaked(true);
			ConnectorOnTaskThreadGroup connectorOnTaskThreadGroup = new ConnectorOnTaskThreadGroup(dataProcessorContext);
			taskNodeInfo.setNodeThreadGroup(connectorOnTaskThreadGroup);
			ConcurrentHashSet<TaskNodeInfo> taskNodeInfos=new ConcurrentHashSet<>();
			taskNodeInfos.add(taskNodeInfo);
			Node node = mock(Node.class);
			when(node.getId()).thenReturn(nodeName);
			when(hazelcastPdkBaseNode.getNode()).thenReturn(node);
			hazelcastPdkBaseNode.getReuseOrNewThreadGroup(taskNodeInfos);
		}
		@Test
		void test2(){
			String nodeName = "leakNode";
			Node leakNode = mock(Node.class);
			when(leakNode.getId()).thenReturn(nodeName);
			TaskNodeInfo taskNodeInfo = new TaskNodeInfo();
			taskNodeInfo.setNode(leakNode);
			ConnectorOnTaskThreadGroup connectorOnTaskThreadGroup = new ConnectorOnTaskThreadGroup(dataProcessorContext);
			taskNodeInfo.setNodeThreadGroup(connectorOnTaskThreadGroup);
			ConcurrentHashSet<TaskNodeInfo> taskNodeInfos=new ConcurrentHashSet<>();
			taskNodeInfos.add(taskNodeInfo);
			Node node = mock(Node.class);
			when(node.getId()).thenReturn(nodeName);
			when(hazelcastPdkBaseNode.getNode()).thenReturn(node);
			hazelcastPdkBaseNode.getReuseOrNewThreadGroup(taskNodeInfos);
		}
	}

	@Test
	void testCreateRetryLifeCycle() {
		HazelcastPdkBaseNode baseNode = new HazelcastSourcePdkDataNode(dataProcessorContext);
		Assertions.assertNotNull(baseNode.createRetryLifeCycle());

		try (MockedStatic<AspectUtils> mockAspect = mockStatic(AspectUtils.class)) {

			AtomicReference<RetryLifeCycleAspect> aspect = new AtomicReference<>();
			mockAspect.when(() -> AspectUtils.executeDataFuncAspect(eq(RetryLifeCycleAspect.class), any(Callable.class), any(CommonUtils.AnyErrorConsumer.class)))
					.then(answer -> {

						Callable arg1 = answer.getArgument(1);
						CommonUtils.AnyErrorConsumer arg2 = answer.getArgument(2);

						Object result = arg1.call();
						Assertions.assertNotNull(result);

						arg2.accept(result);

						aspect.set((RetryLifeCycleAspect) result);

						return null;
					});

			RetryLifeCycle retryLifeCycle = baseNode.createRetryLifeCycle();

			retryLifeCycle.startRetry(15, false, 10, TimeUnit.SECONDS, "WRITE");

			Assertions.assertNotNull(aspect.get().isRetrying());
			Assertions.assertEquals(true, aspect.get().isRetrying());
		}
	}
	
}

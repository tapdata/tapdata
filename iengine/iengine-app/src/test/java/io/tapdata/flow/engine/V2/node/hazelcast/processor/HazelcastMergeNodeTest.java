package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.map.IMap;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Edge;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.flow.engine.V2.task.preview.node.HazelcastPreviewMergeNode;
import io.tapdata.utils.AppType;
import com.tapdata.entity.Connections;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.error.TaskMergeProcessorExCode_16;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.apache.commons.collections4.MapUtils;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-22 15:19
 **/
@DisplayName("HazelcastMergeNode Class Test")
public class HazelcastMergeNodeTest extends BaseHazelcastNodeTest {

	HazelcastMergeNode hazelcastMergeNode;
	MergeTableNode mergeTableNode;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		mergeTableNode = new MergeTableNode();
		mergeTableNode.setMergeProperties(new ArrayList<>());
		when(dataProcessorContext.getNode()).thenReturn((Node) mergeTableNode);
		CommonUtils.setProperty("app_type", "DAAS");
		hazelcastMergeNode = new HazelcastMergeNode(dataProcessorContext);
		ReflectionTestUtils.setField(hazelcastMergeNode, "obsLogger", mockObsLogger);
		ReflectionTestUtils.setField(hazelcastMergeNode, "clientMongoOperator", mockClientMongoOperator);
	}

	@Nested
	@DisplayName("DoInit method test")
	class DoInitTest {
		@BeforeEach
		void setup() {
			hazelcastMergeNode = spy(hazelcastMergeNode);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).initShareJoinKeys();
		}

		@Test
		@DisplayName("Init external storage when app type is 'DFS'")
		void testDoInitExternalStorageCloudAppType() {
			AppType appType = AppType.DFS;
			ExternalStorageDto externalStorageDto = mock(ExternalStorageDto.class);
			try (
					MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class, CALLS_REAL_METHODS);
					MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class);
					MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = mockStatic(PDKIntegration.class);
					MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)
			) {
				ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
				ObsLogger nodeLogger = mock(ObsLogger.class);
				when(obsLoggerFactory.getObsLogger(anyString(), anyString())).thenReturn(nodeLogger);
				obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
				appTypeMockedStatic.when(AppType::currentType).thenReturn(appType);
				externalStorageUtilMockedStatic.when(() -> ExternalStorageUtil.getTargetNodeExternalStorage(
						dataProcessorContext.getNode(),
						dataProcessorContext.getEdges(),
						mockClientMongoOperator,
						dataProcessorContext.getNodes()
				)).thenReturn(externalStorageDto);
				pdkIntegrationMockedStatic.when(() -> PDKIntegration.registerMemoryFetcher(anyString(), any(HazelcastMergeNode.class))).thenAnswer((Answer<Void>) invocation -> null);
				hazelcastMergeNode.doInit(jetContext);
				externalStorageUtilMockedStatic.verify(() -> ExternalStorageUtil.getTargetNodeExternalStorage(
						dataProcessorContext.getNode(),
						dataProcessorContext.getEdges(),
						mockClientMongoOperator,
						dataProcessorContext.getNodes()
				), times(1));
				Object actualObj = ReflectionTestUtils.getField(hazelcastMergeNode, "externalStorageDto");
				assertNotNull(actualObj);
				assertEquals(externalStorageDto, actualObj);
			}
		}

		@Test
		@DisplayName("Init external storage when app type is 'DAAS'")
		void testDoInitExternalStorageDaasAppType() {
			AppType appType = AppType.DAAS;
			ExternalStorageDto externalStorageDto = mock(ExternalStorageDto.class);
			try (
					MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class, CALLS_REAL_METHODS);
					MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class);
					MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = mockStatic(PDKIntegration.class);
					MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)
			) {
				ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
				ObsLogger nodeLogger = mock(ObsLogger.class);
				when(obsLoggerFactory.getObsLogger(anyString(), anyString())).thenReturn(nodeLogger);
				obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
				appTypeMockedStatic.when(AppType::currentType).thenReturn(appType);
				externalStorageUtilMockedStatic.when(() -> ExternalStorageUtil.getTargetNodeExternalStorage(
						dataProcessorContext.getNode(),
						dataProcessorContext.getEdges(),
						null,
						dataProcessorContext.getNodes()
				)).thenReturn(externalStorageDto);
				pdkIntegrationMockedStatic.when(() -> PDKIntegration.registerMemoryFetcher(anyString(), any(HazelcastMergeNode.class))).thenAnswer((Answer<Void>) invocation -> null);
				hazelcastMergeNode.doInit(jetContext);
				externalStorageUtilMockedStatic.verify(() -> ExternalStorageUtil.getTargetNodeExternalStorage(
						dataProcessorContext.getNode(),
						dataProcessorContext.getEdges(),
						null,
						dataProcessorContext.getNodes()
				), new Times(0));
				Object actualObj = ReflectionTestUtils.getField(hazelcastMergeNode, "externalStorageDto");
				assertNull(actualObj);
			}
		}

		@Test
		void testInitMergeTableProperties() {
			when(dataProcessorContext.getNode()).thenReturn((Node) tableNode);
			try {
				hazelcastMergeNode.initMergeTableProperties();
			} catch (TapCodeException e) {
				assertEquals(TaskMergeProcessorExCode_16.WRONG_NODE_TYPE, e.getCode());
			}
		}

	}

	@Test
	void testGetSourceTableNode() {
		MergeTableNode mergeTableNode1 = mock(MergeTableNode.class);
		when(dataProcessorContext.getNode()).thenReturn((Node) mergeTableNode1);
		Graph graph = mock(Graph.class);
		when(mergeTableNode1.getGraph()).thenReturn(graph);
		try {
			hazelcastMergeNode.getSourceTableNode("sourceId");
		} catch (TapCodeException e) {
			assertEquals(TaskMergeProcessorExCode_16.CANNOT_FOUND_PRE_NODE, e.getCode());
		}
	}

	@Nested
	@DisplayName("UpsertCache Method Test")
	class UpsertCacheTest {

		private TapdataEvent tapdataEvent;
		private TapInsertRecordEvent tapInsertRecordEvent;
		private ConstructIMap<Document> constructIMap;
		private MergeTableProperties mergeTableProperties;
		private Map<String, List<String>> sourcePkOrUniqueFieldMap;

		@BeforeEach
		void beforeEach() {
			hazelcastMergeNode = spy(hazelcastMergeNode);
			tapdataEvent = mock(TapdataEvent.class);
			tapInsertRecordEvent = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent.getTableId()).thenReturn("tableName");
			List<String> nodeIds = new ArrayList<>();
			nodeIds.add("sourceId");
			when(tapdataEvent.getNodeIds()).thenReturn(nodeIds);
			when(tapdataEvent.getTapEvent()).thenReturn(tapInsertRecordEvent);
			constructIMap = mock(ConstructIMap.class);
			mergeTableProperties = mock(MergeTableProperties.class);
			sourcePkOrUniqueFieldMap = new HashMap<>();
			TableNode tableNode = mock(TableNode.class);
			when(tableNode.getTableName()).thenReturn("tableName");
			when(tableNode.getName()).thenReturn("NodeName");
			when(tableNode.getId()).thenReturn("sourceId");

			List<Node> nodes = new ArrayList<>();
			nodes.add(tableNode);

			when(dataProcessorContext.getNodes()).thenReturn(nodes);
			when(hazelcastMergeNode.getPreNode("sourceId")).thenReturn(nodes.get(0));
			ExternalStorageDto externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setType("Mongo");
			externalStorageDto.setName("MongoEx");
			ReflectionTestUtils.setField(hazelcastMergeNode, "externalStorageDto", externalStorageDto);
			ReflectionTestUtils.setField(hazelcastMergeNode, "sourcePkOrUniqueFieldMap", sourcePkOrUniqueFieldMap);
			ReflectionTestUtils.setField(hazelcastMergeNode, "mapIterator", new AllLayerMapIterator());
		}

		@Test
		@SneakyThrows
		@DisplayName("Upsert one event into cache must call transformDateTime method")
		void testUpsertOneEventCacheMustCallDateTimeValue() {
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("create_time", new DateTime(Instant.now()));
			when(tapInsertRecordEvent.getAfter()).thenReturn(after);
			List<Map<String, String>> joinKeys = new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}};
			when(mergeTableProperties.getJoinKeys()).thenReturn(joinKeys);
			when(mergeTableProperties.getId()).thenReturn("sourceId");
			sourcePkOrUniqueFieldMap.put("sourceId", new ArrayList<String>() {{
				add("id");
			}});
			when(constructIMap.find(anyString())).thenReturn(null);
			when(constructIMap.upsert(anyString(), any(Document.class))).thenAnswer(invocationOnMock -> null);
			hazelcastMergeNode.upsertCache(tapdataEvent, mergeTableProperties, constructIMap);
			verify(hazelcastMergeNode, new Times(1)).transformDateTime(any(Map.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("Upsert one event into cache must call transformDateTime method")
		void testUpsertOneEventCacheThrowFindException() {
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("create_time", new DateTime(Instant.now()));
			when(tapInsertRecordEvent.getAfter()).thenReturn(after);
			List<Map<String, String>> joinKeys = new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}};
			when(mergeTableProperties.getJoinKeys()).thenReturn(joinKeys);
			when(mergeTableProperties.getId()).thenReturn("sourceId");
			sourcePkOrUniqueFieldMap.put("sourceId", new ArrayList<String>() {{
				add("id");
			}});
			when(constructIMap.find(anyString())).thenThrow(new RuntimeException("connect Exception"));
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastMergeNode.upsertCache(tapdataEvent, mergeTableProperties, constructIMap);
			});
			assertEquals(TaskMergeProcessorExCode_16.UPSERT_CACHE_FIND_BY_JOIN_KEY_FAILED, tapCodeException.getCode());
		}

		@Test
		@SneakyThrows
		@DisplayName("Upsert one event into cache must call transformDateTime method")
		void testUpsertOneEventThrowUpsertException() {
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("create_time", "");
			when(tapInsertRecordEvent.getAfter()).thenReturn(after);
			List<Map<String, String>> joinKeys = new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}};
			when(mergeTableProperties.getJoinKeys()).thenReturn(joinKeys);
			when(mergeTableProperties.getId()).thenReturn("sourceId");
			sourcePkOrUniqueFieldMap.put("sourceId", new ArrayList<String>() {{
				add("id");
			}});
			when(constructIMap.find(anyString())).thenReturn(null);
			when(constructIMap.upsert(anyString(), any(Document.class))).thenThrow(new RuntimeException("throw Upsert Exception"));
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastMergeNode.upsertCache(tapdataEvent, mergeTableProperties, constructIMap);
			});
			assertEquals(TaskMergeProcessorExCode_16.UPSERT_CACHE_FAILED, tapCodeException.getCode());

		}

		@Test
		@SneakyThrows
		@DisplayName("Upsert many events into cache must call transformDateTime method")
		void testUpsertManyEventCacheMustCallDateTimeValue() {
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("create_time", new DateTime(Instant.now()));
			when(tapInsertRecordEvent.getAfter()).thenReturn(after);
			TapInsertRecordEvent tapInsertRecordEvent1 = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent1.getAfter()).thenReturn(new HashMap<String, Object>() {{
				put("id", 2);
				put("create_time", new DateTime(Instant.now()));
			}});
			TapdataEvent tapdataEvent1 = mock(TapdataEvent.class);
			when(tapdataEvent1.getTapEvent()).thenReturn(tapInsertRecordEvent1);
			List<Map<String, String>> joinKeys = new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}};
			when(mergeTableProperties.getJoinKeys()).thenReturn(joinKeys);
			when(mergeTableProperties.getId()).thenReturn("sourceId");
			sourcePkOrUniqueFieldMap.put("sourceId", new ArrayList<String>() {{
				add("id");
			}});
			when(constructIMap.findAll(any(Set.class))).thenReturn(new HashMap<>());
			when(constructIMap.upsert(anyString(), any(Document.class))).thenAnswer(invocationOnMock -> null);
			hazelcastMergeNode.upsertCache(new ArrayList<TapdataEvent>() {{
				add(tapdataEvent);
				add(tapdataEvent1);
			}}, mergeTableProperties, constructIMap);
			verify(hazelcastMergeNode, new Times(2)).transformDateTime(any(Map.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("Upsert many events into cache must call transformDateTime method")
		void testUpsertManyEventWriteFailed() {
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("create_time", new DateTime(Instant.now()));
			when(tapInsertRecordEvent.getAfter()).thenReturn(after);
			TapInsertRecordEvent tapInsertRecordEvent1 = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent1.getAfter()).thenReturn(new HashMap<String, Object>() {{
				put("id", 2);
				put("create_time", new DateTime(Instant.now()));
			}});
			TapdataEvent tapdataEvent1 = mock(TapdataEvent.class);
			when(tapdataEvent1.getTapEvent()).thenReturn(tapInsertRecordEvent1);
			List<Map<String, String>> joinKeys = new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}};
			when(mergeTableProperties.getJoinKeys()).thenReturn(joinKeys);
			when(mergeTableProperties.getId()).thenReturn("sourceId");
			sourcePkOrUniqueFieldMap.put("sourceId", new ArrayList<String>() {{
				add("id");
			}});
			when(constructIMap.findAll(any(Set.class))).thenReturn(new HashMap<>());
			when(constructIMap.insertMany(anyMap())).thenThrow(new RuntimeException("writeFailed"));
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastMergeNode.upsertCache(new ArrayList<TapdataEvent>() {{
					add(tapdataEvent);
					add(tapdataEvent1);
				}}, mergeTableProperties, constructIMap);
			});
			assertEquals(TaskMergeProcessorExCode_16.UPSERT_CACHES_FAILED, tapCodeException.getCode());
		}

		@Test
		@SneakyThrows
		@DisplayName("Upsert many events into cache, when findAll throw InterruptedException")
		void testUpsertManyEventCacheFindAllThrowInterruptedException() {
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("create_time", new DateTime(Instant.now()));
			when(tapInsertRecordEvent.getAfter()).thenReturn(after);
			TapInsertRecordEvent tapInsertRecordEvent1 = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent1.getAfter()).thenReturn(new HashMap<String, Object>() {{
				put("id", 2);
				put("create_time", new DateTime(Instant.now()));
			}});
			TapdataEvent tapdataEvent1 = mock(TapdataEvent.class);
			when(tapdataEvent1.getTapEvent()).thenReturn(tapInsertRecordEvent1);
			List<Map<String, String>> joinKeys = new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}};
			when(mergeTableProperties.getJoinKeys()).thenReturn(joinKeys);
			when(mergeTableProperties.getId()).thenReturn("sourceId");
			sourcePkOrUniqueFieldMap.put("sourceId", new ArrayList<String>() {{
				add("id");
			}});
			when(constructIMap.findAll(any(Set.class))).thenThrow(new RuntimeException(new InterruptedException()));
			assertDoesNotThrow(() -> hazelcastMergeNode.upsertCache(new ArrayList<TapdataEvent>() {{
				add(tapdataEvent);
				add(tapdataEvent1);
			}}, mergeTableProperties, constructIMap));
			verify(hazelcastMergeNode, new Times(0)).transformDateTime(any(Map.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("Upsert many events into cache, when findAll throw Exception")
		void testUpsertManyEventCacheFindAllThrowException() {
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("create_time", new DateTime(Instant.now()));
			when(tapInsertRecordEvent.getAfter()).thenReturn(after);
			TapInsertRecordEvent tapInsertRecordEvent1 = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent1.getAfter()).thenReturn(new HashMap<String, Object>() {{
				put("id", 2);
				put("create_time", new DateTime(Instant.now()));
			}});
			TapdataEvent tapdataEvent1 = mock(TapdataEvent.class);
			when(tapdataEvent1.getTapEvent()).thenReturn(tapInsertRecordEvent1);
			List<Map<String, String>> joinKeys = new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}};
			when(mergeTableProperties.getJoinKeys()).thenReturn(joinKeys);
			when(mergeTableProperties.getId()).thenReturn("sourceId");
			sourcePkOrUniqueFieldMap.put("sourceId", new ArrayList<String>() {{
				add("id");
			}});
			RuntimeException mockEx = new RuntimeException();
			when(constructIMap.findAll(any(Set.class))).thenThrow(mockEx);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastMergeNode.upsertCache(new ArrayList<TapdataEvent>() {{
				add(tapdataEvent);
				add(tapdataEvent1);
			}}, mergeTableProperties, constructIMap));
			verify(hazelcastMergeNode, new Times(0)).transformDateTime(any(Map.class));
			assertEquals(TaskMergeProcessorExCode_16.UPSERT_CACHE_FIND_BY_JOIN_KEYS_FAILED, tapCodeException.getCode());
			assertEquals(mockEx, tapCodeException.getCause());
		}
	}

	@Nested
	@DisplayName("TransformDateTime Method Test")
	class TransformDateTimeTest {
		@Test
		@DisplayName("Main process test")
		void transformDateTime() {
			ReflectionTestUtils.setField(hazelcastMergeNode, "mapIterator", new AllLayerMapIterator());
			Instant now = Instant.now();
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("create_time", new DateTime(now));
			after.put("sub_map", new HashMap<String, Object>() {{
				put("create_time", new DateTime(now));
			}});
			after.put("sub_list", new ArrayList<Object>() {{
				add(new HashMap<String, Object>() {{
					put("create_time", new DateTime(now));
					put("create_time1", new DateTime(now));
				}});
			}});
			hazelcastMergeNode.transformDateTime(after);
			assertEquals(4, after.size());
			assertInstanceOf(Instant.class, after.get("create_time"));
		}

		@Test
		@DisplayName("test illegal date")
		void transformDateTimeWithIllegal() {
			ReflectionTestUtils.setField(hazelcastMergeNode, "mapIterator", new AllLayerMapIterator());
			Instant now = Instant.now();
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			DateTime value = mock(DateTime.class);
			after.put("create_time", value);
			when(value.isContainsIllegal()).thenReturn(true);
			when(value.getIllegalDate()).thenReturn("0000-00-00 00:00:00");
			hazelcastMergeNode.transformDateTime(after);
			assertEquals(2, after.size());
			assertEquals("0000-00-00 00:00:00", after.get("create_time"));
		}
	}

	@Nested
	@DisplayName("initShareJoinKeys Method Test")
	class initShareJoinKeysTest {
		@Test
		@DisplayName("main process test")
		void mainProcessTest() {
			// Mock merge table properties
			List<MergeTableProperties> mergeTableProperties = json2Pojo("mergenode" + File.separator + "init_share_join_keys_properties.json", new TypeReference<List<MergeTableProperties>>() {
			});
			mergeTableNode.setMergeProperties(mergeTableProperties);

			// call test method
			hazelcastMergeNode.initMergeTableProperties();
			hazelcastMergeNode.initShareJoinKeys();

			// assert
			Object actualObj = ReflectionTestUtils.getField(hazelcastMergeNode, "shareJoinKeysMap");
			assertNotNull(actualObj);
			assertInstanceOf(HashMap.class, actualObj);
			Map actualMap = (HashMap) actualObj;
			assertEquals(3, actualMap.size());
			assertTrue(actualMap.containsKey("2"));
			assertTrue(actualMap.containsKey("4"));
			Object actualValue = actualMap.get("2");
			assertNotNull(actualValue);
			assertInstanceOf(HashSet.class, actualValue);
			assertEquals(4, ((HashSet) actualValue).size());
			assertTrue(((HashSet) actualValue).contains("city_id"));
			assertTrue(((HashSet) actualValue).contains("name"));
			assertTrue(((HashSet) actualValue).contains("xxx_id"));
			actualValue = actualMap.get("4");
			assertNotNull(actualValue);
			assertInstanceOf(HashSet.class, actualValue);
			assertEquals(2, ((HashSet) actualValue).size());
			assertTrue(((HashSet) actualValue).contains("xxx.xxx_id"));
		}
	}

	@Nested
	@DisplayName("joinKeyExists method test")
	class joinKeyExistsTest {
		@BeforeEach
		void beforeEach() {
			List<MergeTableProperties> mergeTableProperties = json2Pojo("mergenode" + File.separator + "init_share_join_keys_properties.json", new TypeReference<List<MergeTableProperties>>() {
			});
			mergeTableNode.setMergeProperties(mergeTableProperties);
			hazelcastMergeNode.initMergeTableProperties();
		}

		@Test
		@DisplayName("main process test")
		void mainProcessTest() {
			boolean actual = hazelcastMergeNode.joinKeyExists("city_id", HazelcastMergeNode.JoinConditionType.TARGET);
			assertTrue(actual);
		}

		@Test
		@DisplayName("join condition type is source")
		void joinConditionTypeIsSource() {
			boolean actual = hazelcastMergeNode.joinKeyExists("xxx.xxx_id", HazelcastMergeNode.JoinConditionType.SOURCE);
			assertTrue(actual);
		}

		@Test
		@DisplayName("first parameter[joinKey] is null or empty")
		void firstParameterJoinKeyIsNullOrEmpty() {
			boolean actual = hazelcastMergeNode.joinKeyExists(null, HazelcastMergeNode.JoinConditionType.TARGET);
			assertFalse(actual);
			actual = hazelcastMergeNode.joinKeyExists("", HazelcastMergeNode.JoinConditionType.TARGET);
			assertFalse(actual);
		}

		@Test
		@DisplayName("second parameter[joinConditionType] is null")
		void secondParameterJoinConditionTypeIsNull() {
			boolean actual = hazelcastMergeNode.joinKeyExists("city_id", null);
			assertFalse(actual);
		}

		@Test
		@DisplayName("mergeTablePropertiesMap is null")
		void mergeTablePropertiesMapIsNull() {
			ReflectionTestUtils.setField(hazelcastMergeNode, "mergeTablePropertiesMap", null);
			boolean actual = hazelcastMergeNode.joinKeyExists("city_id", HazelcastMergeNode.JoinConditionType.TARGET);
			assertFalse(actual);
		}
	}

	@Nested
	@DisplayName("initMergeTablePropertyReferenceMap method test")
	class initMergeTablePropertyReferenceMapTest {
		class MockHazelcastMergeNode extends HazelcastMergeNode {
			public MockHazelcastMergeNode(DataProcessorContext dataProcessorContext) {
				super(dataProcessorContext);
			}

			@Override
			protected boolean isRunning() {
				return true;
			}
		}

		@Test
		@DisplayName("main process test")
		@SneakyThrows
		void testMainProcess() {
			List<MergeTableProperties> mergeTableProperties = json2Pojo("mergenode" + File.separator + "initMergeTablePropertyReferenceMap.json", new TypeReference<List<MergeTableProperties>>() {
			});
			mergeTableNode.setMergeProperties(mergeTableProperties);
			TapTable a = new TapTable("a").add(new TapField().name("a_id")).add(new TapField().name("a_id1"));
			TapTable b = new TapTable("b").add(new TapField().name("b_id"));
			TapTable c = new TapTable("c").add(new TapField().name("c_id"));
			TapTable d = new TapTable("d").add(new TapField().name("d_id")).add(new TapField().name("d_id1")).add(new TapField().name("d_id2"));
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get("a")).thenReturn(a);
			when(tapTableMap.get("b")).thenReturn(b);
			when(tapTableMap.get("c")).thenReturn(c);
			when(tapTableMap.get("d")).thenReturn(d);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			MockHazelcastMergeNode mockHazelcastMergeNode = new MockHazelcastMergeNode(dataProcessorContext);
			mockHazelcastMergeNode = spy(mockHazelcastMergeNode);
			TableNode aNode = new TableNode();
			aNode.setTableName("a");
			TableNode bNode = new TableNode();
			bNode.setTableName("b");
			TableNode cNode = new TableNode();
			cNode.setTableName("c");
			TableNode dNode = new TableNode();
			dNode.setTableName("d");
			doReturn(aNode).when(mockHazelcastMergeNode).getPreNode("1");
			doReturn(bNode).when(mockHazelcastMergeNode).getPreNode("2");
			doReturn(cNode).when(mockHazelcastMergeNode).getPreNode("3");
			doReturn(dNode).when(mockHazelcastMergeNode).getPreNode("4");

			mockHazelcastMergeNode.initFirstLevelIds();
			mockHazelcastMergeNode.initMergeTableProperties();
			mockHazelcastMergeNode.initMergeTablePropertyReferenceMap();
			Object mergeTablePropertyReferenceMapObj = ReflectionTestUtils.getField(mockHazelcastMergeNode, "mergeTablePropertyReferenceMap");
			assertInstanceOf(HashMap.class, mergeTablePropertyReferenceMapObj);
			HashMap mergeTablePropertyReferenceMap = (HashMap) mergeTablePropertyReferenceMapObj;
			assertEquals(4, mergeTablePropertyReferenceMap.size());

			assertTrue(mergeTablePropertyReferenceMap.containsKey("1"));
			Object referenceObj = mergeTablePropertyReferenceMap.get("1");
			assertInstanceOf(HazelcastMergeNode.MergeTablePropertyReference.class, referenceObj);
			HazelcastMergeNode.MergeTablePropertyReference reference = (HazelcastMergeNode.MergeTablePropertyReference) referenceObj;
			assertNull(reference.getParentJoinKeyReferences());
			assertNotNull(reference.getChildJoinKeyReferences());
			List<HazelcastMergeNode.JoinKeyReference> references = reference.getChildJoinKeyReferences();
			assertEquals(2, references.size());
			HazelcastMergeNode.JoinKeyReference joinKeyReference = references.get(0);
			assertNotNull(joinKeyReference);
			assertEquals("2", joinKeyReference.getMergeTableProperties().getId());
			Map<String, String> referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			assertEquals(1, referenceJoinKeys.size());
			assertTrue(referenceJoinKeys.containsKey("a_id"));
			assertEquals("b_id", referenceJoinKeys.get("a_id"));
			assertNotNull(joinKeyReference.getChildJoinKeyReferences());
			List<HazelcastMergeNode.JoinKeyReference> childJoinKeyReferences = joinKeyReference.getChildJoinKeyReferences();
			assertEquals(1, childJoinKeyReferences.size());
			joinKeyReference = childJoinKeyReferences.get(0);
			assertNotNull(joinKeyReference);
			assertEquals("4", joinKeyReference.getMergeTableProperties().getId());
			referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			assertEquals(2, referenceJoinKeys.size());
			assertTrue(referenceJoinKeys.containsKey("a_id"));
			assertEquals("d_id1", referenceJoinKeys.get("a_id"));
			assertTrue(referenceJoinKeys.containsKey("a_id1"));
			assertEquals("d_id2", referenceJoinKeys.get("a_id1"));
			joinKeyReference = references.get(1);
			assertNotNull(joinKeyReference);
			assertEquals("3", joinKeyReference.getMergeTableProperties().getId());
			referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			assertEquals(1, referenceJoinKeys.size());
			assertTrue(referenceJoinKeys.containsKey("a_id"));
			assertEquals("c_id", referenceJoinKeys.get("a_id"));
			assertNull(joinKeyReference.getChildJoinKeyReferences());

			referenceObj = mergeTablePropertyReferenceMap.get("2");
			assertInstanceOf(HazelcastMergeNode.MergeTablePropertyReference.class, referenceObj);
			reference = (HazelcastMergeNode.MergeTablePropertyReference) referenceObj;
			assertNotNull(reference.getParentJoinKeyReferences());
			assertNull(joinKeyReference.getChildJoinKeyReferences());
			references = reference.getParentJoinKeyReferences();
			assertEquals(1, references.size());
			joinKeyReference = references.get(0);
			assertNotNull(joinKeyReference);
			assertEquals("1", joinKeyReference.getMergeTableProperties().getId());
			referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			assertEquals(1, referenceJoinKeys.size());
			assertTrue(referenceJoinKeys.containsKey("b_id"));
			assertEquals("a_id", referenceJoinKeys.get("b_id"));
			references = reference.getChildJoinKeyReferences();
			assertEquals(1, references.size());
			joinKeyReference = references.get(0);
			assertNotNull(joinKeyReference);
			assertEquals("4", joinKeyReference.getMergeTableProperties().getId());
			referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			assertEquals(1, referenceJoinKeys.size());
			assertTrue(referenceJoinKeys.containsKey("b_id"));
			assertEquals("d_id", referenceJoinKeys.get("b_id"));

			referenceObj = mergeTablePropertyReferenceMap.get("3");
			assertInstanceOf(HazelcastMergeNode.MergeTablePropertyReference.class, referenceObj);
			reference = (HazelcastMergeNode.MergeTablePropertyReference) referenceObj;
			assertNotNull(reference.getParentJoinKeyReferences());
			assertNull(joinKeyReference.getChildJoinKeyReferences());
			references = reference.getParentJoinKeyReferences();
			assertEquals(1, references.size());
			joinKeyReference = references.get(0);
			assertNotNull(joinKeyReference);
			assertEquals("1", joinKeyReference.getMergeTableProperties().getId());
			referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			assertEquals(1, referenceJoinKeys.size());
			assertTrue(referenceJoinKeys.containsKey("c_id"));
			assertEquals("a_id", referenceJoinKeys.get("c_id"));

			referenceObj = mergeTablePropertyReferenceMap.get("4");
			assertInstanceOf(HazelcastMergeNode.MergeTablePropertyReference.class, referenceObj);
			reference = (HazelcastMergeNode.MergeTablePropertyReference) referenceObj;
			assertNotNull(reference.getParentJoinKeyReferences());
			assertNull(joinKeyReference.getChildJoinKeyReferences());
			references = reference.getParentJoinKeyReferences();
			assertEquals(2, references.size());
			joinKeyReference = references.get(0);
			assertNotNull(joinKeyReference);
			assertEquals("2", joinKeyReference.getMergeTableProperties().getId());
			referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			assertEquals(1, referenceJoinKeys.size());
			assertTrue(referenceJoinKeys.containsKey("d_id"));
			assertEquals("b_id", referenceJoinKeys.get("d_id"));
			joinKeyReference = references.get(1);
			assertNotNull(joinKeyReference);
			assertEquals("1", joinKeyReference.getMergeTableProperties().getId());
			referenceJoinKeys = joinKeyReference.getReferenceJoinKeys();
			assertEquals(2, referenceJoinKeys.size());
			assertTrue(referenceJoinKeys.containsKey("d_id1"));
			assertEquals("a_id", referenceJoinKeys.get("d_id1"));
			assertTrue(referenceJoinKeys.containsKey("d_id2"));
			assertEquals("a_id1", referenceJoinKeys.get("d_id2"));

			Object enableUpdateJoinKeyMapObj = ReflectionTestUtils.getField(mockHazelcastMergeNode, "enableUpdateJoinKeyMap");
			assertInstanceOf(HashMap.class, enableUpdateJoinKeyMapObj);
			HashMap enableUpdateJoinKeyMap = (HashMap) enableUpdateJoinKeyMapObj;
			assertEquals(4, enableUpdateJoinKeyMap.size());

			Object enableUpdateJoinKeyObj = enableUpdateJoinKeyMap.get("1");
			assertInstanceOf(HazelcastMergeNode.EnableUpdateJoinKey.class, enableUpdateJoinKeyObj);
			HazelcastMergeNode.EnableUpdateJoinKey enableUpdateJoinKey = (HazelcastMergeNode.EnableUpdateJoinKey) enableUpdateJoinKeyObj;
			assertFalse(enableUpdateJoinKey.isEnableParent());
			assertTrue(enableUpdateJoinKey.isEnableChildren());

			enableUpdateJoinKeyObj = enableUpdateJoinKeyMap.get("2");
			assertInstanceOf(HazelcastMergeNode.EnableUpdateJoinKey.class, enableUpdateJoinKeyObj);
			enableUpdateJoinKey = (HazelcastMergeNode.EnableUpdateJoinKey) enableUpdateJoinKeyObj;
			assertTrue(enableUpdateJoinKey.isEnableParent());
			assertTrue(enableUpdateJoinKey.isEnableChildren());

			enableUpdateJoinKeyObj = enableUpdateJoinKeyMap.get("3");
			assertInstanceOf(HazelcastMergeNode.EnableUpdateJoinKey.class, enableUpdateJoinKeyObj);
			enableUpdateJoinKey = (HazelcastMergeNode.EnableUpdateJoinKey) enableUpdateJoinKeyObj;
			assertFalse(enableUpdateJoinKey.isEnableParent());
			assertFalse(enableUpdateJoinKey.isEnableChildren());

			enableUpdateJoinKeyObj = enableUpdateJoinKeyMap.get("4");
			assertInstanceOf(HazelcastMergeNode.EnableUpdateJoinKey.class, enableUpdateJoinKeyObj);
			enableUpdateJoinKey = (HazelcastMergeNode.EnableUpdateJoinKey) enableUpdateJoinKeyObj;
			assertTrue(enableUpdateJoinKey.isEnableParent());
			assertFalse(enableUpdateJoinKey.isEnableChildren());
		}

		@Test
		@DisplayName("when mergeTablePropertiesMap is null")
		void mergeTablePropertiesMapIsNull() {
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastMergeNode.initMergeTablePropertyReferenceMap());
			assertEquals(TaskMergeProcessorExCode_16.INIT_MERGE_PROPERTY_RREFERENCE_FAILED_MERGE_PROPERTIES_MAP_IS_NULL, tapCodeException.getCode());
		}
	}

	@Nested
	@DisplayName("initCheckJoinKeyUpdateCacheMap method test")
	class initCheckJoinKeyUpdateCacheMapTest {

		private MockHazelcastMergeNode mockHazelcastMergeNode;
		private ConstructIMap constructIMap;
		private TapTableMap tapTableMap;

		class MockHazelcastMergeNode extends HazelcastMergeNode {
			public MockHazelcastMergeNode(DataProcessorContext dataProcessorContext) {
				super(dataProcessorContext);
			}

			@Override
			protected boolean isRunning() {
				return true;
			}
		}

		@BeforeEach
		@SneakyThrows
		void setUp() {
			List<MergeTableProperties> mergeTableProperties = json2Pojo("mergenode" + File.separator + "initMergeTablePropertyReferenceMap.json", new TypeReference<List<MergeTableProperties>>() {
			});
			mergeTableNode.setMergeProperties(mergeTableProperties);
			TapTable a = new TapTable("a").add(new TapField().name("a_id")).add(new TapField().name("a_id1"));
			TapTable b = new TapTable("b").add(new TapField().name("b_id"));
			TapTable c = new TapTable("c").add(new TapField().name("c_id"));
			TapTable d = new TapTable("d").add(new TapField().name("d_id")).add(new TapField().name("d_id1")).add(new TapField().name("d_id2"));
			tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get("a")).thenReturn(a);
			when(tapTableMap.get("b")).thenReturn(b);
			when(tapTableMap.get("c")).thenReturn(c);
			when(tapTableMap.get("d")).thenReturn(d);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			mockHazelcastMergeNode = new MockHazelcastMergeNode(dataProcessorContext);
			mockHazelcastMergeNode = spy(mockHazelcastMergeNode);
			TableNode aNode = new TableNode();
			aNode.setTableName("a");
			TableNode bNode = new TableNode();
			bNode.setName("b");
			bNode.setTableName("b");
			TableNode cNode = new TableNode();
			cNode.setTableName("c");
			TableNode dNode = new TableNode();
			dNode.setTableName("d");
			doReturn(aNode).when(mockHazelcastMergeNode).getPreNode("1");
			doReturn(bNode).when(mockHazelcastMergeNode).getPreNode("2");
			doReturn(cNode).when(mockHazelcastMergeNode).getPreNode("3");
			doReturn(dNode).when(mockHazelcastMergeNode).getPreNode("4");
			Capability capability = new Capability(ConnectionOptions.CAPABILITY_SOURCE_INCREMENTAL_UPDATE_EVENT_HAVE_BEFORE);
			List<Capability> capabilities = new ArrayList<>();
			capabilities.add(capability);
			Connections aConn = new Connections();
			aConn.setCapabilities(capabilities);
			Connections bConn = new Connections();
			Connections cConn = new Connections();
			cConn.setCapabilities(capabilities);
			Connections dConn = new Connections();
			dConn.setCapabilities(capabilities);
			Map<String, Connections> connectionsMap = new HashMap<>();
			connectionsMap.put("1", aConn);
			connectionsMap.put("2", bConn);
			connectionsMap.put("3", cConn);
			connectionsMap.put("4", dConn);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "sourceConnectionMap", connectionsMap);
			CommonUtils.setProperty(HazelcastMergeNode.UPDATE_JOIN_KEY_VALUE_CACHE_IN_MEM_SIZE_PROP_KEY, "100");
			ExternalStorageDto externalStorageDto = mock(ExternalStorageDto.class);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "externalStorageDto", externalStorageDto);
			constructIMap = mock(ConstructIMap.class);
			when(constructIMap.isEmpty()).thenReturn(true);
			when(constructIMap.insert(anyString(), any())).thenAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(1);
				assertInstanceOf(Document.class, argument1);
				Document document = (Document) argument1;
				assertEquals(3, document.size());
				return 1;
			});
			Processor.Context jetContext = mock(Processor.Context.class);
			HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
			when(jetContext.hazelcastInstance()).thenReturn(hazelcastInstance);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "jetContext", jetContext);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "obsLogger", mockObsLogger);

			mockHazelcastMergeNode.initFirstLevelIds();
			mockHazelcastMergeNode.initMergeTableProperties();
			mockHazelcastMergeNode.initMergeTablePropertyReferenceMap();
		}

		@Test
		@SneakyThrows
		@DisplayName("main process test")
		void testMainProcess() {
			try (MockedStatic<HazelcastMergeNode> hazelcastMergeNodeMockedStatic = mockStatic(HazelcastMergeNode.class)) {
				hazelcastMergeNodeMockedStatic.when(() -> HazelcastMergeNode.buildConstructIMap(any(), anyString(), anyString(), any())).thenReturn(constructIMap);
				hazelcastMergeNodeMockedStatic.when(() -> HazelcastMergeNode.getCheckUpdateJoinKeyValueCacheName(any())).thenReturn("Merge_Test");
				Map<String, Connections> connectionsMap = (Map<String, Connections>) ReflectionTestUtils.getField(mockHazelcastMergeNode, "sourceConnectionMap");
				Capability capability = new Capability(ConnectionOptions.CAPABILITY_SOURCE_INCREMENTAL_UPDATE_EVENT_HAVE_BEFORE);
				List<Capability> capabilities = new ArrayList<>();
				capabilities.add(capability);
				Connections bConn = new Connections();
				bConn.setCapabilities(capabilities);
				connectionsMap.put("2", bConn);
				mockHazelcastMergeNode.initCheckJoinKeyUpdateCacheMap();
				Object checkJoinKeyUpdateCacheMapObj = ReflectionTestUtils.getField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap");
				assertInstanceOf(HashMap.class, checkJoinKeyUpdateCacheMapObj);
				HashMap checkJoinKeyUpdateCacheMap = (HashMap) checkJoinKeyUpdateCacheMapObj;
				assertEquals(0, checkJoinKeyUpdateCacheMap.size());
				assertFalse(checkJoinKeyUpdateCacheMap.containsKey("2"));
			}
		}

		@Test
		@DisplayName("when join key include pk")
		void whenJoinKeyIncludePK() {
			try (MockedStatic<HazelcastMergeNode> hazelcastMergeNodeMockedStatic = mockStatic(HazelcastMergeNode.class)) {
				hazelcastMergeNodeMockedStatic.when(() -> HazelcastMergeNode.buildConstructIMap(any(), anyString(), anyString(), any())).thenReturn(constructIMap);
				hazelcastMergeNodeMockedStatic.when(() -> HazelcastMergeNode.getCheckUpdateJoinKeyValueCacheName(any())).thenReturn("Merge_Test");
				ObsLogger nodeLogger = mock(ObsLogger.class);
				ReflectionTestUtils.setField(mockHazelcastMergeNode, "nodeLogger", nodeLogger);
				TapTable b = tapTableMap.get("b");
				b.getNameFieldMap().get("b_id").primaryKeyPos(1);
				assertDoesNotThrow(() -> mockHazelcastMergeNode.initCheckJoinKeyUpdateCacheMap());
				verify(nodeLogger).warn(any(), any());
			}
		}
	}

	@Nested
	@DisplayName("handleUpdateJoinKey method test")
	class handleBatchUpdateJoinKeyIfNeedTest {
		@BeforeEach
		void setUp() {
			hazelcastMergeNode = spy(hazelcastMergeNode);
		}

		@Test
		@DisplayName("when enable parent update join key")
		void whenEnableParentUpdateJoinKey() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(TapInsertRecordEvent.create().init());
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			HazelcastMergeNode.EnableUpdateJoinKey enableUpdateJoinKey = new HazelcastMergeNode.EnableUpdateJoinKey();
			enableUpdateJoinKey.enableParent();
			doReturn("1").when(hazelcastMergeNode).getPreNodeId(tapdataEvent);
			Map<String, HazelcastMergeNode.EnableUpdateJoinKey> enableUpdateJoinKeyMap = new HashMap<>();
			enableUpdateJoinKeyMap.put("1", enableUpdateJoinKey);
			ReflectionTestUtils.setField(hazelcastMergeNode, "enableUpdateJoinKeyMap", enableUpdateJoinKeyMap);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKey(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKey(batchEventWrappers);
			verify(hazelcastMergeNode, times(1)).handleUpdateJoinKey(tapdataEvent);
		}

		@Test
		@DisplayName("when enable children update join key")
		void whenEnableChildrenUpdateJoinKey() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(TapInsertRecordEvent.create().init());
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			HazelcastMergeNode.EnableUpdateJoinKey enableUpdateJoinKey = new HazelcastMergeNode.EnableUpdateJoinKey();
			enableUpdateJoinKey.enableChildren();
			doReturn("1").when(hazelcastMergeNode).getPreNodeId(tapdataEvent);
			Map<String, HazelcastMergeNode.EnableUpdateJoinKey> enableUpdateJoinKeyMap = new HashMap<>();
			enableUpdateJoinKeyMap.put("1", enableUpdateJoinKey);
			ReflectionTestUtils.setField(hazelcastMergeNode, "enableUpdateJoinKeyMap", enableUpdateJoinKeyMap);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKey(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKey(batchEventWrappers);
			verify(hazelcastMergeNode, times(1)).handleUpdateJoinKey(tapdataEvent);
		}

		@Test
		@DisplayName("when enable both parent and children update join key")
		void whenEnableBothParentAndChildrenUpdateJoinKey() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(TapInsertRecordEvent.create().init());
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			HazelcastMergeNode.EnableUpdateJoinKey enableUpdateJoinKey = new HazelcastMergeNode.EnableUpdateJoinKey();
			enableUpdateJoinKey.enableChildren();
			enableUpdateJoinKey.enableParent();
			doReturn("1").when(hazelcastMergeNode).getPreNodeId(tapdataEvent);
			Map<String, HazelcastMergeNode.EnableUpdateJoinKey> enableUpdateJoinKeyMap = new HashMap<>();
			enableUpdateJoinKeyMap.put("1", enableUpdateJoinKey);
			ReflectionTestUtils.setField(hazelcastMergeNode, "enableUpdateJoinKeyMap", enableUpdateJoinKeyMap);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKey(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKey(batchEventWrappers);
			verify(hazelcastMergeNode, times(1)).handleUpdateJoinKey(tapdataEvent);
		}

		@Test
		@DisplayName("when disable update join key")
		void whenDisableUpdateJoinKey() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			TapdataEvent tapdataEvent = new TapdataEvent();
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			HazelcastMergeNode.EnableUpdateJoinKey enableUpdateJoinKey = new HazelcastMergeNode.EnableUpdateJoinKey();
			doReturn("1").when(hazelcastMergeNode).getPreNodeId(tapdataEvent);
			Map<String, HazelcastMergeNode.EnableUpdateJoinKey> enableUpdateJoinKeyMap = new HashMap<>();
			enableUpdateJoinKeyMap.put("1", enableUpdateJoinKey);
			ReflectionTestUtils.setField(hazelcastMergeNode, "enableUpdateJoinKeyMap", enableUpdateJoinKeyMap);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKey(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKey(batchEventWrappers);
			verify(hazelcastMergeNode, times(0)).handleUpdateJoinKey(tapdataEvent);
		}

		@Test
		@DisplayName("when enableUpdateJoinKey is null")
		void whenEnableUpdateJoinKeyIsNull() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			TapdataEvent tapdataEvent = new TapdataEvent();
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			doReturn("1").when(hazelcastMergeNode).getPreNodeId(tapdataEvent);
			Map<String, HazelcastMergeNode.EnableUpdateJoinKey> enableUpdateJoinKeyMap = new HashMap<>();
			enableUpdateJoinKeyMap.put("1", null);
			ReflectionTestUtils.setField(hazelcastMergeNode, "enableUpdateJoinKeyMap", enableUpdateJoinKeyMap);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKey(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKey(batchEventWrappers);
			verify(hazelcastMergeNode, times(0)).handleUpdateJoinKey(tapdataEvent);
		}

		@Test
		@DisplayName("when input batch event list is null or empty")
		void whenInputBatchEventsNullOrEmpty() {
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKey(any());
			assertDoesNotThrow(() -> hazelcastMergeNode.handleBatchUpdateJoinKey(null));
			verify(hazelcastMergeNode, times(0)).handleUpdateJoinKey(any());

			assertDoesNotThrow(() -> hazelcastMergeNode.handleBatchUpdateJoinKey(new ArrayList<>()));
			verify(hazelcastMergeNode, times(0)).handleUpdateJoinKey(any());
		}
	}

	@Nested
	@DisplayName("handleUpdateJoinKeyIfNeed method test")
	class handleUpdateJoinKeyIfNeedTest {
		private MockHazelcastMergeNode mockHazelcastMergeNode;
		private TapTableMap tapTableMap;
		private TableNode aNode;
		private TableNode bNode;
		private TableNode cNode;
		private TableNode dNode;
		private TableNode eNode;
		private Connections aConn;

		class MockHazelcastMergeNode extends HazelcastMergeNode {
			public MockHazelcastMergeNode(DataProcessorContext dataProcessorContext) {
				super(dataProcessorContext);
			}

			@Override
			protected boolean isRunning() {
				return true;
			}
		}

		@BeforeEach
		@SneakyThrows
		void setUp() {
			List<MergeTableProperties> mergeTableProperties = json2Pojo("mergenode" + File.separator + "handleUpdateJoinKeyIfNeed.json", new TypeReference<List<MergeTableProperties>>() {
			});
			mergeTableNode.setMergeProperties(mergeTableProperties);
			TapTable a = new TapTable("a").add(new TapField().name("a_id")).add(new TapField().name("a_id1")).add(new TapField().name("cid")).add(new TapField().name("eid")).add(new TapField().name("id").primaryKeyPos(1));
			TapTable b = new TapTable("b").add(new TapField().name("b_id"));
			TapTable c = new TapTable("c").add(new TapField().name("c_id"));
			TapTable d = new TapTable("d").add(new TapField().name("d_id")).add(new TapField().name("d_id1")).add(new TapField().name("d_id2"));
			TapTable e = new TapTable("e").add(new TapField().name("e_id"));
			tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get("a")).thenReturn(a);
			when(tapTableMap.get("b")).thenReturn(b);
			when(tapTableMap.get("c")).thenReturn(c);
			when(tapTableMap.get("d")).thenReturn(d);
			when(tapTableMap.get("e")).thenReturn(e);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			mockHazelcastMergeNode = new MockHazelcastMergeNode(dataProcessorContext);
			mockHazelcastMergeNode = spy(mockHazelcastMergeNode);
			aNode = new TableNode();
			aNode.setName("a node");
			aNode.setTableName("a");
			bNode = new TableNode();
			bNode.setName("b node");
			bNode.setTableName("b");
			cNode = new TableNode();
			cNode.setTableName("c");
			dNode = new TableNode();
			dNode.setTableName("d");
			eNode = new TableNode();
			dNode.setTableName("e");
			doReturn(aNode).when(mockHazelcastMergeNode).getPreNode("1");
			doReturn(bNode).when(mockHazelcastMergeNode).getPreNode("2");
			doReturn(cNode).when(mockHazelcastMergeNode).getPreNode("3");
			doReturn(dNode).when(mockHazelcastMergeNode).getPreNode("4");
			doReturn(eNode).when(mockHazelcastMergeNode).getPreNode("5");
			Capability capability = new Capability(ConnectionOptions.CAPABILITY_SOURCE_INCREMENTAL_UPDATE_EVENT_HAVE_BEFORE);
			List<Capability> capabilities = new ArrayList<>();
			capabilities.add(capability);
			aConn = new Connections();
			aConn.setCapabilities(capabilities);
			Connections bConn = new Connections();
			Connections cConn = new Connections();
			cConn.setCapabilities(capabilities);
			Connections dConn = new Connections();
			dConn.setCapabilities(capabilities);
			Connections eConn = new Connections();
			eConn.setCapabilities(capabilities);
			Map<String, Connections> connectionsMap = new HashMap<>();
			connectionsMap.put("1", aConn);
			connectionsMap.put("2", bConn);
			connectionsMap.put("3", cConn);
			connectionsMap.put("4", dConn);
			connectionsMap.put("5", eConn);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "sourceConnectionMap", connectionsMap);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "obsLogger", mockObsLogger);

			mockHazelcastMergeNode.initSourceNodeLevelMap(null, 1);
			mockHazelcastMergeNode.initFirstLevelIds();
			mockHazelcastMergeNode.initMergeTableProperties();
			mockHazelcastMergeNode.initShareJoinKeys();
			mockHazelcastMergeNode.initMergeTablePropertyReferenceMap();

			doAnswer(invocationOnMock -> null).when(mockHazelcastMergeNode).removeMergeCacheIfUpdateJoinKey(any(), any());
		}

		@Test
		@DisplayName("update parent join key")
		void testUpdateParentJoinKey() {
			Map<String, Object> before = new HashMap<>();
			before.put("b_id", 1);
			Map<String, Object> after = new HashMap<>();
			after.put("b_id", 2);
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
			tapUpdateRecordEvent.setBefore(before);
			tapUpdateRecordEvent.setAfter(after);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			doReturn("2").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);

			mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent);
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertInstanceOf(MergeInfo.class, mergeInfoObj);
			MergeInfo mergeInfo = (MergeInfo) mergeInfoObj;
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = mergeInfo.getUpdateJoinKeys();
			assertTrue(MapUtils.isNotEmpty(updateJoinKeys));
			assertTrue(updateJoinKeys.containsKey("2"));
			MergeInfo.UpdateJoinKey updateJoinKey = updateJoinKeys.get("2");
			Map<String, Object> before1 = updateJoinKey.getBefore();
			Map<String, Object> after1 = updateJoinKey.getAfter();
			assertEquals(1, before1.size());
			assertTrue(before1.containsKey("b_id"));
			assertEquals(1, before1.get("b_id"));
			assertEquals(1, after1.size());
			assertTrue(after1.containsKey("b_id"));
			assertEquals(2, after1.get("b_id"));
		}

		@Test
		@DisplayName("not update parent join key")
		void testNotUpdateParentJoinKey() {
			Map<String, Object> before = new HashMap<>();
			before.put("b_id", 1);
			Map<String, Object> after = new HashMap<>();
			after.put("b_id", 1);
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
			tapUpdateRecordEvent.setBefore(before);
			tapUpdateRecordEvent.setAfter(after);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			doReturn("2").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);

			mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent);
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertNull(mergeInfoObj);
		}

		@Test
		@DisplayName("update children join key")
		void testUpdateChildrenJoinKey() {
			Map<String, Object> before = new HashMap<>();
			before.put("a_id", 1);
			before.put("a_id1", 1);
			before.put("cid", 1);
			before.put("id", 1);
			Map<String, Object> after = new HashMap<>();
			after.put("a_id", 1);
			after.put("a_id1", 2);
			after.put("cid", 2);
			after.put("id", 1);
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
			tapUpdateRecordEvent.setBefore(before);
			tapUpdateRecordEvent.setAfter(after);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			doReturn("1").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);

			mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent);
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertInstanceOf(MergeInfo.class, mergeInfoObj);
			MergeInfo mergeInfo = (MergeInfo) mergeInfoObj;

			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = mergeInfo.getUpdateJoinKeys();
			assertTrue(MapUtils.isNotEmpty(updateJoinKeys));
			assertTrue(updateJoinKeys.containsKey("3"));
			MergeInfo.UpdateJoinKey updateJoinKey = updateJoinKeys.get("3");
			Map<String, Object> before1 = updateJoinKey.getBefore();
			Map<String, Object> after1 = updateJoinKey.getAfter();
			assertEquals(1, before1.size());
			assertTrue(before1.containsKey("cid"));
			assertEquals(1, before1.get("cid"));
			assertEquals(1, after1.size());
			assertTrue(after1.containsKey("cid"));
			assertEquals(2, after1.get("cid"));

			assertTrue(updateJoinKeys.containsKey("4"));
			updateJoinKey = updateJoinKeys.get("4");
			before1 = updateJoinKey.getBefore();
			after1 = updateJoinKey.getAfter();
			assertEquals(2, before1.size());
			assertTrue(before1.containsKey("a_id"));
			assertTrue(before1.containsKey("a_id1"));
			assertEquals(1, before1.get("a_id"));
			assertEquals(1, before1.get("a_id1"));
			assertEquals(2, after1.size());
			assertTrue(after1.containsKey("a_id"));
			assertTrue(after1.containsKey("a_id1"));
			assertEquals(1, after1.get("a_id"));
			assertEquals(2, after1.get("a_id1"));
		}

		@Test
		@DisplayName("not update children join key")
		void testNotUpdateChildrenJoinKey() {
			Map<String, Object> before = new HashMap<>();
			before.put("a_id", 1);
			before.put("a_id1", 1);
			before.put("cid", 1);
			Map<String, Object> after = new HashMap<>();
			after.put("a_id", 1);
			after.put("a_id1", 1);
			after.put("cid", 1);
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
			tapUpdateRecordEvent.setBefore(before);
			tapUpdateRecordEvent.setAfter(after);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			doReturn("1").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);

			mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent);
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertNull(mergeInfoObj);
		}

		@Test
		@SneakyThrows
		@DisplayName("when event not have before")
		void testEventNotHaveBefore() {
			Map<String, Object> after = new HashMap<>();
			after.put("a_id", 1);
			after.put("a_id1", 2);
			after.put("cid", 2);
			after.put("id", 1);
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
			tapUpdateRecordEvent.setAfter(after);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			doReturn("1").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = new HashMap<>();
			ConstructIMap constructIMap = mock(ConstructIMap.class);
			checkJoinKeyUpdateCacheMap.put("1", constructIMap);
			Document before = new Document()
					.append("a_id", 1)
					.append("a_id1", 1)
					.append("cid", 1)
					.append("id", 1);
			when(constructIMap.find("1")).thenReturn(before);
			when(constructIMap.upsert(eq("1"), any(Document.class))).thenAnswer(invocationOnMock -> {
				Object argument2 = invocationOnMock.getArgument(1);
				assertInstanceOf(Document.class, argument2);
				Document document = (Document) argument2;
				assertEquals(4, document.size());
				return null;
			});
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);
			Map<String, List<String>> sourcePkOrUniqueFieldMap = new HashMap<>();
			sourcePkOrUniqueFieldMap.put("1", Collections.singletonList("id"));
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "sourcePkOrUniqueFieldMap", sourcePkOrUniqueFieldMap);
			AllLayerMapIterator mapIterator = new AllLayerMapIterator();
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "mapIterator", mapIterator);
			aConn.setCapabilities(new ArrayList<>());

			mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent);
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertInstanceOf(MergeInfo.class, mergeInfoObj);
			MergeInfo mergeInfo = (MergeInfo) mergeInfoObj;

			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = mergeInfo.getUpdateJoinKeys();
			assertTrue(MapUtils.isNotEmpty(updateJoinKeys));
			assertTrue(updateJoinKeys.containsKey("3"));
			MergeInfo.UpdateJoinKey updateJoinKey = updateJoinKeys.get("3");
			Map<String, Object> before1 = updateJoinKey.getBefore();
			Map<String, Object> after1 = updateJoinKey.getAfter();
			assertEquals(1, before1.size());
			assertTrue(before1.containsKey("cid"));
			assertEquals(1, before1.get("cid"));
			assertEquals(1, after1.size());
			assertTrue(after1.containsKey("cid"));
			assertEquals(2, after1.get("cid"));

			assertTrue(updateJoinKeys.containsKey("4"));
			updateJoinKey = updateJoinKeys.get("4");
			before1 = updateJoinKey.getBefore();
			after1 = updateJoinKey.getAfter();
			assertEquals(2, before1.size());
			assertTrue(before1.containsKey("a_id"));
			assertTrue(before1.containsKey("a_id1"));
			assertEquals(1, before1.get("a_id"));
			assertEquals(1, before1.get("a_id1"));
			assertEquals(2, after1.size());
			assertTrue(after1.containsKey("a_id"));
			assertTrue(after1.containsKey("a_id1"));
			assertEquals(1, after1.get("a_id"));
			assertEquals(2, after1.get("a_id1"));
		}

		@Test
		@SneakyThrows
		@DisplayName("when after is null or empty")
		void afterIsNullOrEmpty() {
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
			tapUpdateRecordEvent.setAfter(null);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			doReturn("1").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent));

			tapUpdateRecordEvent.setAfter(new HashMap<>());
			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent));
		}

		@Test
		@DisplayName("when input tapdata event is null")
		void testTapdataEventIsNull() {
			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKey(null));
		}

		@Test
		@DisplayName("when input tapdata event is not dml")
		void testNotDMLEvent() {
			TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapAlterFieldNameEvent);
			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent));
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertNull(mergeInfoObj);
		}

		@Test
		@DisplayName("when is insert record event and have cache")
		void testIsInsertRecordEventAndHaveCache() {
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			when(checkJoinKeyUpdateCacheMap.containsKey("2")).thenReturn(true);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			doReturn("2").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertNotNull(argument1);
				assertEquals(tapdataEvent, argument1);
				return null;
			}).when(mockHazelcastMergeNode).insertJoinKeyCache(tapdataEvent);

			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent));
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertNull(mergeInfoObj);
			verify(mockHazelcastMergeNode, times(1)).insertJoinKeyCache(tapdataEvent);
		}

		@Test
		@DisplayName("when is insert record event and not have cache")
		void testIsInsertRecordEventAndNotHaveCache() {
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			when(checkJoinKeyUpdateCacheMap.containsKey("2")).thenReturn(false);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			doReturn("2").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			doAnswer(invocationOnMock -> null).when(mockHazelcastMergeNode).insertJoinKeyCache(tapdataEvent);

			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent));
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertNull(mergeInfoObj);
			verify(mockHazelcastMergeNode, times(0)).insertJoinKeyCache(tapdataEvent);
		}

		@Test
		@DisplayName("when is delete record event and have cache")
		void testIsDeleteRecordEventAndHaveCache() {
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			when(checkJoinKeyUpdateCacheMap.containsKey("2")).thenReturn(true);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);
			TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapDeleteRecordEvent);
			doReturn("2").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertNotNull(argument1);
				assertEquals(tapdataEvent, argument1);
				return null;
			}).when(mockHazelcastMergeNode).deleteJoinKeyCache(tapdataEvent);

			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent));
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertNull(mergeInfoObj);
			verify(mockHazelcastMergeNode, times(1)).deleteJoinKeyCache(tapdataEvent);
		}

		@Test
		@DisplayName("when is delete record event and not have cache")
		void testIsDeleteRecordEventAndNotHaveCache() {
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			when(checkJoinKeyUpdateCacheMap.containsKey("2")).thenReturn(false);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);
			TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapDeleteRecordEvent);
			doReturn("2").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			doAnswer(invocationOnMock -> null).when(mockHazelcastMergeNode).deleteJoinKeyCache(tapdataEvent);

			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKey(tapdataEvent));
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertNull(mergeInfoObj);
			verify(mockHazelcastMergeNode, times(0)).deleteJoinKeyCache(tapdataEvent);
		}
	}

	@Nested
	@DisplayName("insertJoinKeyCache method test")
	class insertJoinKeyCacheTest {

		private HazelcastMergeNode mock;

		@BeforeEach
		void setUp() {
			mock = spy(hazelcastMergeNode);
		}

		@Test
		@DisplayName("main process test")
		@SneakyThrows
		void testMainProcess() {
			Map<String, Object> after = new HashMap<>();
			after.put("code", "a");
			after.put("id", 1);
			after.put("test", "test");
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			tapInsertRecordEvent.setAfter(after);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			doReturn("1").when(mock).getPreNodeId(tapdataEvent);
			doAnswer(invocationOnMock -> null).when(mock).transformDateTime(after);
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			ConstructIMap<Document> constructIMap = mock(ConstructIMap.class);
			doReturn("a_1").when(mock).getPkOrUniqueValueKey(after, "1", constructIMap);
			when(constructIMap.insert(eq("a_1"), any(Document.class))).thenAnswer(invocationOnMock -> {
				Object argument2 = invocationOnMock.getArgument(1);
				assertInstanceOf(Document.class, argument2);
				assertEquals(3, ((Document) argument2).size());
				return null;
			});
			when(checkJoinKeyUpdateCacheMap.get("1")).thenReturn(constructIMap);
			ReflectionTestUtils.setField(mock, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);

			mock.insertJoinKeyCache(tapdataEvent);
			verify(mock, times(1)).transformDateTime(after);
			verify(constructIMap, times(1)).insert(eq("a_1"), any(Document.class));
		}

		@Test
		@DisplayName("when cache map is null")
		void constructIMapIsNull() {
			Map<String, Object> after = new HashMap<>();
			after.put("code", "a");
			after.put("id", 1);
			after.put("test", "test");
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			tapInsertRecordEvent.setAfter(after);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			doReturn("1").when(mock).getPreNodeId(tapdataEvent);
			Node node = mock(Node.class);
			when(node.getName()).thenReturn("test");
			doReturn(node).when(mock).getPreNode("1");
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			when(checkJoinKeyUpdateCacheMap.get("1")).thenReturn(null);
			ReflectionTestUtils.setField(mock, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);

			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> mock.insertJoinKeyCache(tapdataEvent));
			assertEquals(TaskMergeProcessorExCode_16.INSERT_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP, tapCodeException.getCode());
		}

		@Test
		@DisplayName("when insert cache error")
		@SneakyThrows
		void insertCacheError() {
			Map<String, Object> after = new HashMap<>();
			after.put("code", "a");
			after.put("id", 1);
			after.put("test", "test");
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			tapInsertRecordEvent.setAfter(after);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			doReturn("1").when(mock).getPreNodeId(tapdataEvent);
			doAnswer(invocationOnMock -> null).when(mock).transformDateTime(after);
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			ConstructIMap<Document> constructIMap = mock(ConstructIMap.class);
			when(constructIMap.getName()).thenReturn("cache");
			doReturn("a_1").when(mock).getPkOrUniqueValueKey(after, "1", constructIMap);
			RuntimeException runtimeException = new RuntimeException("test");
			when(constructIMap.insert(eq("a_1"), any(Document.class))).thenThrow(runtimeException);
			when(checkJoinKeyUpdateCacheMap.get("1")).thenReturn(constructIMap);
			ReflectionTestUtils.setField(mock, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);

			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> mock.insertJoinKeyCache(tapdataEvent));
			assertEquals(TaskMergeProcessorExCode_16.INSERT_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED, tapCodeException.getCode());
			assertEquals(runtimeException, tapCodeException.getCause());
		}
	}

	@Nested
	@DisplayName("deleteJoinKeyCache method test")
	class deleteJoinKeyCacheTest {

		private HazelcastMergeNode mockHazelcastMergeNode;

		@BeforeEach
		void setUp() {
			mockHazelcastMergeNode = spy(hazelcastMergeNode);
		}

		@Test
		@DisplayName("main process test")
		@SneakyThrows
		void testMainProcess() {
			Map<String, Object> before = new HashMap<>();
			before.put("code", "a");
			before.put("id", 1);
			before.put("test", "test");
			TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
			tapDeleteRecordEvent.setBefore(before);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapDeleteRecordEvent);
			doReturn("1").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			doAnswer(invocationOnMock -> null).when(mockHazelcastMergeNode).transformDateTime(before);
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			ConstructIMap<Document> constructIMap = mock(ConstructIMap.class);
			when(constructIMap.getName()).thenReturn("cache");
			doReturn("a_1").when(mockHazelcastMergeNode).getPkOrUniqueValueKey(before, "1", constructIMap);
			when(constructIMap.delete(eq("a_1"))).thenAnswer(invocationOnMock -> null);
			when(checkJoinKeyUpdateCacheMap.get("1")).thenReturn(constructIMap);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);

			mockHazelcastMergeNode.deleteJoinKeyCache(tapdataEvent);
			verify(mockHazelcastMergeNode, times(1)).transformDateTime(before);
			verify(constructIMap, times(1)).delete(eq("a_1"));
		}

		@Test
		@DisplayName("when cache is null")
		@SneakyThrows
		void cacheIsNull() {
			Map<String, Object> before = new HashMap<>();
			before.put("code", "a");
			before.put("id", 1);
			before.put("test", "test");
			TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
			tapDeleteRecordEvent.setBefore(before);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapDeleteRecordEvent);
			doReturn("1").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			Node node = mock(Node.class);
			when(node.getName()).thenReturn("test");
			doReturn(node).when(mockHazelcastMergeNode).getPreNode("1");
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			when(checkJoinKeyUpdateCacheMap.get("1")).thenReturn(null);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);

			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> mockHazelcastMergeNode.deleteJoinKeyCache(tapdataEvent));
			assertEquals(TaskMergeProcessorExCode_16.DELETE_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP, tapCodeException.getCode());
		}

		@Test
		@DisplayName("when delete cache error")
		@SneakyThrows
		void deleteCacheError() {
			Map<String, Object> before = new HashMap<>();
			before.put("code", "a");
			before.put("id", 1);
			before.put("test", "test");
			TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
			tapDeleteRecordEvent.setBefore(before);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapDeleteRecordEvent);
			doReturn("1").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			doAnswer(invocationOnMock -> null).when(mockHazelcastMergeNode).transformDateTime(before);
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			ConstructIMap<Document> constructIMap = mock(ConstructIMap.class);
			when(constructIMap.getName()).thenReturn("cache");
			doReturn("a_1").when(mockHazelcastMergeNode).getPkOrUniqueValueKey(before, "1", constructIMap);
			RuntimeException runtimeException = new RuntimeException("test");
			when(constructIMap.delete(eq("a_1"))).thenThrow(runtimeException);
			when(checkJoinKeyUpdateCacheMap.get("1")).thenReturn(constructIMap);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);

			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> mockHazelcastMergeNode.deleteJoinKeyCache(tapdataEvent));
			assertEquals(TaskMergeProcessorExCode_16.DELETE_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED, tapCodeException.getCode());
			assertEquals(runtimeException, tapCodeException.getCause());
		}

		@Test
		@DisplayName("when before not have pk field(s)")
		@SneakyThrows
		void beforeNotHavePk() {
			Map<String, Object> before = new HashMap<>();
			TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
			tapDeleteRecordEvent.setBefore(before);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapDeleteRecordEvent);
			doReturn("1").when(mockHazelcastMergeNode).getPreNodeId(tapdataEvent);
			doAnswer(invocationOnMock -> null).when(mockHazelcastMergeNode).transformDateTime(before);
			Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap = mock(Map.class);
			ConstructIMap<Document> constructIMap = mock(ConstructIMap.class);
			when(constructIMap.getName()).thenReturn("cache");
			doReturn("").when(mockHazelcastMergeNode).getPkOrUniqueValueKey(before, "1", constructIMap);
			when(checkJoinKeyUpdateCacheMap.get("1")).thenReturn(constructIMap);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);

			mockHazelcastMergeNode.deleteJoinKeyCache(tapdataEvent);
			verify(constructIMap, times(0)).delete(anyString());
		}
	}

	@Nested
	@DisplayName("transformToTapValue method test")
	class transformToTapValueTest {
		private HazelcastMergeNode mockHazelcastMergeNode;

		@BeforeEach
		void setUp() {
			mockHazelcastMergeNode = spy(hazelcastMergeNode);
		}

		@Test
		@DisplayName("main process test")
		void testMainProcess() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			MergeInfo mergeInfo = new MergeInfo();
			List<MergeLookupResult> mergeLookupResult = new ArrayList<>();
			mergeInfo.setMergeLookupResults(mergeLookupResult);
			TapEvent tapEvent = mock(TapEvent.class);
			when(tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY)).thenReturn(mergeInfo);
			tapdataEvent.setTapEvent(tapEvent);
			doAnswer(invocationOnMock -> null).when(mockHazelcastMergeNode).recursiveMergeInfoTransformToTapValue(mergeLookupResult);

			mockHazelcastMergeNode.transformToTapValue(tapdataEvent, null, null, null);

			verify(mockHazelcastMergeNode, times(1)).recursiveMergeInfoTransformToTapValue(mergeLookupResult);
		}

		@Test
		@DisplayName("when TapEvent not have MergeInfo")
		void testNotHaveMergeInfo() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			TapEvent tapEvent = mock(TapEvent.class);
			when(tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY)).thenReturn(null);
			tapdataEvent.setTapEvent(tapEvent);

			mockHazelcastMergeNode.transformToTapValue(tapdataEvent, null, null, null);
			verify(mockHazelcastMergeNode, times(0)).recursiveMergeInfoTransformToTapValue(any());
		}
	}

	@Nested
	@DisplayName("recursiveMergeInfoTransformToTapValue method test")
	class recursiveMergeInfoTransformToTapValueTest {
		private HazelcastMergeNode mockHazelcastMergeNode;

		@BeforeEach
		void setUp() {
			mockHazelcastMergeNode = spy(hazelcastMergeNode);
			AllLayerMapIterator mapIterator = new AllLayerMapIterator();
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "mapIterator", mapIterator);
		}

		@Test
		void testMainProcess() {
			TapTableMap tapTableMap = mock(TapTableMap.class);
			TapTable tapTable = new TapTable();
			tapTable.putField("_id", new TapField("_id", BsonType.OBJECT_ID.name()));
			tapTable.putField("name", new TapField("name", BsonType.STRING.name()));
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			Node node = mock(Node.class);
			when(node.getId()).thenReturn("1");
			doReturn(node).when(mockHazelcastMergeNode).getPreNode("1");
			when(tapTableMap.get("1")).thenReturn(tapTable);
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
			MergeLookupResult mergeLookupResult = new MergeLookupResult();
			mergeLookupResults.add(mergeLookupResult);
			String id = new ObjectId().toHexString();
			Map<String, Object> data = new HashMap<String, Object>() {{
				put("_id", id);
				put("name", "test");
				put("subDoc", new Document("subId", id));
				put("subList", new ArrayList<Object>() {{
					add(new HashMap<String, Object>() {{
						put("sub_id", id);
					}});
				}});
			}};
			mergeLookupResult.setData(data);
			io.tapdata.pdk.apis.entity.merge.MergeTableProperties mergeTableProperties = new io.tapdata.pdk.apis.entity.merge.MergeTableProperties();
			mergeTableProperties.setId("1");
			mergeLookupResult.setProperty(mergeTableProperties);
			List<MergeLookupResult> childMergeLookupResults = new ArrayList<>();
			MergeLookupResult childMergeLookupResult = new MergeLookupResult();
			childMergeLookupResults.add(childMergeLookupResult);
			Map<String, Object> childData = new HashMap<>(data);
			childMergeLookupResult.setData(childData);
			io.tapdata.pdk.apis.entity.merge.MergeTableProperties childMergeTableProperties = new io.tapdata.pdk.apis.entity.merge.MergeTableProperties();
			childMergeTableProperties.setId("1");
			childMergeLookupResult.setProperty(childMergeTableProperties);
			mergeLookupResult.setMergeLookupResults(childMergeLookupResults);

			mockHazelcastMergeNode.recursiveMergeInfoTransformToTapValue(mergeLookupResults);
			assertInstanceOf(TapStringValue.class, data.get("_id"));
			TapStringValue tapStringValue = (TapStringValue) data.get("_id");
			assertEquals(id, tapStringValue.getValue());
			TapString tapType = tapStringValue.getTapType();
			assertEquals(24L, tapType.getBytes());
			assertTrue(tapType.getFixed());
			assertEquals(BsonType.OBJECT_ID.name(), tapStringValue.getOriginType());
			assertNull(tapStringValue.getOriginValue());
			assertInstanceOf(String.class, data.get("name"));
			assertEquals("test", data.get("name"));
			assertInstanceOf(TapStringValue.class, childData.get("_id"));
			tapStringValue = (TapStringValue) childData.get("_id");
			assertEquals(id, tapStringValue.getValue());
			tapType = tapStringValue.getTapType();
			assertEquals(24L, tapType.getBytes());
			assertTrue(tapType.getFixed());
			assertEquals(BsonType.OBJECT_ID.name(), tapStringValue.getOriginType());
			assertNull(tapStringValue.getOriginValue());
			assertInstanceOf(TapMapValue.class, data.get("subDoc"));
			assertInstanceOf(TapArrayValue.class, data.get("subList"));
		}

		@Test
		@DisplayName("when name fields map is empty")
		void fieldsEmpty() {
			TapTableMap tapTableMap = mock(TapTableMap.class);
			TapTable tapTable = new TapTable();
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			Node node = mock(Node.class);
			when(node.getId()).thenReturn("1");
			doReturn(node).when(mockHazelcastMergeNode).getPreNode("1");
			when(tapTableMap.get("1")).thenReturn(tapTable);
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
			MergeLookupResult mergeLookupResult = new MergeLookupResult();
			mergeLookupResults.add(mergeLookupResult);
			String id = new ObjectId().toHexString();
			Map<String, Object> data = new HashMap<String, Object>() {{
				put("_id", id);
			}};
			mergeLookupResult.setData(data);
			io.tapdata.pdk.apis.entity.merge.MergeTableProperties mergeTableProperties = new io.tapdata.pdk.apis.entity.merge.MergeTableProperties();
			mergeTableProperties.setId("1");
			mergeLookupResult.setProperty(mergeTableProperties);

			mockHazelcastMergeNode.recursiveMergeInfoTransformToTapValue(mergeLookupResults);
			assertInstanceOf(String.class, data.get("_id"));
			assertEquals(id, data.get("_id"));
		}
	}

	@Nested
	@DisplayName("Method needCache test")
	class NeedCacheTest {
		private HazelcastMergeNode mockHazelcastMergeNode;
		List<String> needCacheList = new ArrayList<>();

		@BeforeEach
		void setUp() {
			needCacheList.add("123");
			mockHazelcastMergeNode = spy(hazelcastMergeNode);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "needCacheIdList", needCacheList);
		}

		@DisplayName("test task is initalSync and mergeMode is not subTableFirst")
		@Test
		void test1() {
			processorBaseContext.getTaskDto().setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("123");
			when(mockHazelcastMergeNode.isSubTableFirstMode()).thenReturn(false);
			boolean result = mockHazelcastMergeNode.needCache(tapdataEvent);
			assertEquals(false, result);
		}

		@DisplayName("test task is initalSync and mergeMode is subTableFirst")
		@Test
		void test2() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("123");
			TapUpdateRecordEvent tapUpdateRecordEvent = TapUpdateRecordEvent.create().init();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			when(mockHazelcastMergeNode.isSubTableFirstMode()).thenReturn(true);
			boolean result = mockHazelcastMergeNode.needCache(tapdataEvent);
			assertEquals(true, result);
		}

		@DisplayName("test task is initalSync and mergeMode is subTableFirst ,but nodeList not contain id")
		@Test
		void test3() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("1234");
			TapUpdateRecordEvent tapUpdateRecordEvent = TapUpdateRecordEvent.create().init();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			when(mockHazelcastMergeNode.isSubTableFirstMode()).thenReturn(true);
			boolean result = mockHazelcastMergeNode.needCache(tapdataEvent);
			assertEquals(false, result);
		}
	}

	@Nested
	@DisplayName("Method needLookup test")
	class NeedLookUpTest {
		private HazelcastMergeNode mockHazelcastMergeNode;
		List<String> needCacheList = new ArrayList<>();
		private Map<String, List<MergeTableProperties>> lookupMap = new HashMap<>();
		private Set<String> firstLevelMergeNodeIds = new HashSet<>();
		private Map<String, HazelcastMergeNode.EnableUpdateJoinKey> enableUpdateJoinKeyMap;

		@BeforeEach
		void setUp() {
			needCacheList.add("123");
			mockHazelcastMergeNode = spy(hazelcastMergeNode);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "needCacheIdList", needCacheList);
			List<MergeTableProperties> mergeTableProperties = new ArrayList<>();
			lookupMap.put("123", mergeTableProperties);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "lookupMap", lookupMap);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "firstLevelMergeNodeIds", firstLevelMergeNodeIds);
			enableUpdateJoinKeyMap = new HashMap<>();
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "enableUpdateJoinKeyMap", enableUpdateJoinKeyMap);
		}

		@DisplayName("test task is initalSync and mergeMode is mainTableFirst")
		@Test
		void test1() {
			processorBaseContext.getTaskDto().setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("123");
			TapUpdateRecordEvent tapUpdateRecordEvent = TapUpdateRecordEvent.create().init();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			when(mockHazelcastMergeNode.isSubTableFirstMode()).thenReturn(false);
			boolean result = mockHazelcastMergeNode.needLookup(tapdataEvent);
			assertEquals(false, result);
		}

		@DisplayName("test task is initalSync, mergeMode is subTableFirst and tapevent is first level node")
		@Test
		void test2() {
			firstLevelMergeNodeIds.add("123");
			processorBaseContext.getTaskDto().setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("123");
			tapdataEvent.setSyncStage(SyncStage.INITIAL_SYNC);
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().init();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			when(mockHazelcastMergeNode.isSubTableFirstMode()).thenReturn(true);
			boolean result = mockHazelcastMergeNode.needLookup(tapdataEvent);
			assertEquals(true, result);
		}

		@Test
		@DisplayName("test cdc task, update and delete TapEvent, expect return false")
		void test3() {
			processorBaseContext.getTaskDto().setType(SyncTypeEnum.CDC.getSyncType());
			doReturn(false).when(mockHazelcastMergeNode).isSubTableFirstMode();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("123");
			TapUpdateRecordEvent tapUpdateRecordEvent = TapUpdateRecordEvent.create().init();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			enableUpdateJoinKeyMap.computeIfAbsent("123", k -> new HazelcastMergeNode.EnableUpdateJoinKey());
			assertFalse(mockHazelcastMergeNode.needLookup(tapdataEvent));

			TapDeleteRecordEvent tapDeleteRecordEvent = TapDeleteRecordEvent.create().init();
			tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapDeleteRecordEvent);
			assertFalse(mockHazelcastMergeNode.needLookup(tapdataEvent));
		}

		@Test
		@DisplayName("test cdc update event, and enable update join key is true, expect return true")
		void test4() {
			processorBaseContext.getTaskDto().setType(SyncTypeEnum.CDC.getSyncType());
			doReturn(false).when(mockHazelcastMergeNode).isSubTableFirstMode();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("123");
			TapUpdateRecordEvent tapUpdateRecordEvent = TapUpdateRecordEvent.create().init();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			enableUpdateJoinKeyMap.computeIfAbsent("123", k -> {
				HazelcastMergeNode.EnableUpdateJoinKey enableUpdateJoinKey = new HazelcastMergeNode.EnableUpdateJoinKey();
				enableUpdateJoinKey.enableChildren();
				return enableUpdateJoinKey;
			});
			tapdataEvent.setSyncStage(SyncStage.CDC);
			assertTrue(mockHazelcastMergeNode.needLookup(tapdataEvent));
		}

		@Test
		@DisplayName("test when tapdata event is a signal event")
		void test5() {
			TapdataHeartbeatEvent tapdataHeartbeatEvent = new TapdataHeartbeatEvent();
			assertFalse(mockHazelcastMergeNode.needLookup(tapdataHeartbeatEvent));
		}
	}

	@Nested
	@DisplayName("Method initMergeCache test")
	class InitMergeCacheTest {
		private HazelcastMergeNode mockHazelcastMergeNode;

		@BeforeEach
		void setUp() {
			mockHazelcastMergeNode = spy(hazelcastMergeNode);
		}

		@Test
		void test1() {
			processorBaseContext.getTaskDto().setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			mockHazelcastMergeNode.initMergeCache();
			Map<String, ConstructIMap<Document>> cacheMap = (Map<String, ConstructIMap<Document>>) ReflectionTestUtils.getField(mockHazelcastMergeNode, "mergeCacheMap");
			when(mockHazelcastMergeNode.isSubTableFirstMode()).thenReturn(false);
			assertEquals(null, cacheMap);
		}

		@Test
		void test2() {
			processorBaseContext.getTaskDto().setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			when(mockHazelcastMergeNode.isSubTableFirstMode()).thenReturn(true);
			mockHazelcastMergeNode.initMergeCache();
			Map<String, ConstructIMap<Document>> cacheMap = (Map<String, ConstructIMap<Document>>) ReflectionTestUtils.getField(mockHazelcastMergeNode, "mergeCacheMap");
			boolean mapIsNull = cacheMap != null;
			assertEquals(true, mapIsNull);
		}

		@Test
		void test_INIT_MERGE_CACHE_GET_CACHE_NAME_FAILED() {
			Map<String, List<MergeTableProperties>> lookupMap = new HashMap<>();
			List<MergeTableProperties> mergeTableProperties = new ArrayList<>();
			mergeTableProperties.add(mock(MergeTableProperties.class));
			lookupMap.put("test", mergeTableProperties);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "lookupMap", lookupMap);
			processorBaseContext.getTaskDto().setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			when(mockHazelcastMergeNode.isSubTableFirstMode()).thenReturn(true);
			TapCodeException exception = assertThrows(TapCodeException.class, () -> mockHazelcastMergeNode.initMergeCache());
			assertEquals(TaskMergeProcessorExCode_16.INIT_MERGE_CACHE_GET_CACHE_NAME_FAILED, exception.getCode());
		}
	}

	@Nested
	@DisplayName("Method tryProcess list test")
	class tryProcessListTest {
		@BeforeEach
		void setUp() {
			hazelcastMergeNode = spy(hazelcastMergeNode);
			HazelcastMergeNode.BatchProcessMetrics batchProcessMetrics = mock(HazelcastMergeNode.BatchProcessMetrics.class);
			ReflectionTestUtils.setField(hazelcastMergeNode, "batchProcessMetrics", batchProcessMetrics);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).loggerBeforeProcess(any(List.class));
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleBatchUpdateJoinKey(any(List.class));
			doAnswer(invocation -> null).when(hazelcastMergeNode).wrapMergeInfo(any(TapdataEvent.class));
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).loggerBatchUpdateCache(any(List.class));
			doReturn("test").when(hazelcastMergeNode).getPreTableName(any(TapdataEvent.class));
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).acceptIfNeed(any(Consumer.class), any(List.class), any(List.class));
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			TapdataEvent createIndexEvent = mock(TapdataEvent.class);
			ReflectionTestUtils.setField(hazelcastMergeNode, "createIndexEvent", createIndexEvent);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(TapInsertRecordEvent.create().init());
			TapdataHeartbeatEvent tapdataHeartbeatEvent = new TapdataHeartbeatEvent();
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataHeartbeatEvent));
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			Consumer<List<HazelcastProcessorBaseNode.BatchProcessResult>> consumer = o -> assertEquals(batchEventWrappers.size(), o.size());

			doReturn(true).when(hazelcastMergeNode).needCache(any(TapdataEvent.class));
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertInstanceOf(ArrayList.class, argument1);
				assertEquals(1, ((ArrayList<HazelcastProcessorBaseNode.BatchEventWrapper>) argument1).size());
				return null;
			}).when(hazelcastMergeNode).doBatchCache(any(List.class));
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertInstanceOf(ArrayList.class, argument1);
				assertEquals(batchEventWrappers.size(), ((ArrayList<HazelcastProcessorBaseNode.BatchEventWrapper>) argument1).size());
				return null;
			}).when(hazelcastMergeNode).doBatchLookUpConcurrent(any(List.class), any(List.class));

			assertDoesNotThrow(() -> hazelcastMergeNode.tryProcess(batchEventWrappers, consumer));

			verify(hazelcastMergeNode, times(2)).acceptIfNeed(any(Consumer.class), any(List.class), any(List.class));
			verify(hazelcastMergeNode, times(1)).doBatchCache(any(List.class));
			verify(hazelcastMergeNode, times(1)).doBatchLookUpConcurrent(any(List.class), any(List.class));
		}

		@Test
		@DisplayName("test when create index event is null")
		void testWhenCreateIndexIsNull() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			HazelcastProcessorBaseNode.BatchEventWrapper batchEventWrapper = new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent);
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			batchEventWrappers.add(batchEventWrapper);
			Consumer<List<HazelcastProcessorBaseNode.BatchProcessResult>> consumer = o -> assertEquals(batchEventWrappers.size(), o.size());

			doReturn(true).when(hazelcastMergeNode).needCache(any(TapdataEvent.class));
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertInstanceOf(ArrayList.class, argument1);
				assertEquals(batchEventWrappers.size(), ((ArrayList<HazelcastProcessorBaseNode.BatchEventWrapper>) argument1).size());
				return null;
			}).when(hazelcastMergeNode).doBatchCache(any(List.class));
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertInstanceOf(ArrayList.class, argument1);
				assertEquals(batchEventWrappers.size(), ((ArrayList<HazelcastProcessorBaseNode.BatchEventWrapper>) argument1).size());
				return null;
			}).when(hazelcastMergeNode).doBatchLookUpConcurrent(any(List.class), any(List.class));

			assertDoesNotThrow(() -> hazelcastMergeNode.tryProcess(batchEventWrappers, consumer));

			verify(hazelcastMergeNode, times(1)).acceptIfNeed(any(Consumer.class), any(List.class), any(List.class));
		}
	}

	@Nested
	@DisplayName("Method loggerBeforeProcess test")
	class loggerBeforeProcessTest {

		private ObsLogger nodeLogger;

		@BeforeEach
		void setUp() {
			nodeLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(hazelcastMergeNode, "nodeLogger", nodeLogger);
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			when(nodeLogger.isDebugEnabled()).thenReturn(true);
			List<HazelcastProcessorBaseNode.BatchEventWrapper> list = new ArrayList<>();
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));

			hazelcastMergeNode.loggerBeforeProcess(list);

			verify(nodeLogger, times(2)).debug(anyString(), any(Object[].class));
		}

		@Test
		@DisplayName("test debug not enabled")
		void testDebugNotEnabled() {
			when(nodeLogger.isDebugEnabled()).thenReturn(false);
			List<HazelcastProcessorBaseNode.BatchEventWrapper> list = new ArrayList<>();
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));

			hazelcastMergeNode.loggerBeforeProcess(list);

			verify(nodeLogger, never()).debug(anyString(), any(Object[].class));
		}

		@Test
		@DisplayName("test nodeLogger is null")
		void testNodeLoggerIsNull() {
			ReflectionTestUtils.setField(hazelcastMergeNode, "nodeLogger", null);
			List<HazelcastProcessorBaseNode.BatchEventWrapper> list = new ArrayList<>();
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));

			assertDoesNotThrow(() -> hazelcastMergeNode.loggerBeforeProcess(list));
		}
	}

	@Nested
	@DisplayName("Method doBatchLookUpConcurrent test")
	class doBatchLookUpConcurrentTest {
		@BeforeEach
		void setUp() {
			hazelcastMergeNode = spy(hazelcastMergeNode);
		}

		@Test
		@DisplayName("test main process")
		void mainProcess() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> list = new ArrayList<>();
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			doReturn(true).when(hazelcastMergeNode).needLookup(any(TapdataEvent.class));
			CompletableFuture<Void> completableFuture = mock(CompletableFuture.class);
			doReturn(completableFuture).when(hazelcastMergeNode).lookupAndWrapMergeInfoConcurrent(any(TapdataEvent.class));
			List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

			hazelcastMergeNode.doBatchLookUpConcurrent(list, completableFutures);

			verify(hazelcastMergeNode, times(2)).lookupAndWrapMergeInfoConcurrent(any(TapdataEvent.class));
			assertEquals(list.size(), completableFutures.size());
		}

		@Test
		@DisplayName("test when some event need lookup, some no need lookup")
		void testWhenSomeEventNoNeedLookup() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> list = new ArrayList<>();
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			TapInsertRecordEvent tapInsertRecordEvent1 = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent1 = new TapdataEvent();
			tapdataEvent1.setTapEvent(tapInsertRecordEvent1);
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent1));
			doReturn(true).when(hazelcastMergeNode).needLookup(tapdataEvent);
			doReturn(false).when(hazelcastMergeNode).needLookup(tapdataEvent1);
			CompletableFuture<Void> completableFuture = mock(CompletableFuture.class);
			doReturn(completableFuture).when(hazelcastMergeNode).lookupAndWrapMergeInfoConcurrent(any(TapdataEvent.class));
			List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

			hazelcastMergeNode.doBatchLookUpConcurrent(list, completableFutures);

			verify(hazelcastMergeNode, times(1)).lookupAndWrapMergeInfoConcurrent(any(TapdataEvent.class));
			assertEquals(1, completableFutures.size());
		}

		@Test
		@DisplayName("test input event list is null")
		void testEventListIsNull() {
			assertDoesNotThrow(() -> hazelcastMergeNode.doBatchLookUpConcurrent(null, new ArrayList<>()));
		}

		@Test
		@DisplayName("test input completable future list is null")
		void testCompletableFutureListIsNull() {
			TapCodeException tapCodeException = assertThrows(TapCodeException.class,
					() -> hazelcastMergeNode.doBatchLookUpConcurrent(new ArrayList<>(), null));
			assertEquals(TaskMergeProcessorExCode_16.LOOKUP_COMPLETABLE_FUTURE_LIST_IS_NULL, tapCodeException.getCode());
		}
	}

	@Nested
	@DisplayName("Method loggerBatchUpdateCache test")
	class loggerBatchUpdateCacheTest {
		private ObsLogger nodeLogger;

		@BeforeEach
		void setUp() {
			nodeLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(hazelcastMergeNode, "nodeLogger", nodeLogger);
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			when(nodeLogger.isDebugEnabled()).thenReturn(true);
			List<HazelcastProcessorBaseNode.BatchEventWrapper> list = new ArrayList<>();
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));

			hazelcastMergeNode.loggerBatchUpdateCache(list);

			verify(nodeLogger, times(2)).debug(anyString(), any(Object[].class));
		}

		@Test
		@DisplayName("test debug not enabled")
		void testDebugNotEnabled() {
			when(nodeLogger.isDebugEnabled()).thenReturn(false);
			List<HazelcastProcessorBaseNode.BatchEventWrapper> list = new ArrayList<>();
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));

			hazelcastMergeNode.loggerBatchUpdateCache(list);

			verify(nodeLogger, never()).debug(anyString(), any(Object[].class));
		}

		@Test
		@DisplayName("test nodeLogger is null")
		void testNodeLoggerIsNull() {
			ReflectionTestUtils.setField(hazelcastMergeNode, "nodeLogger", null);
			List<HazelcastProcessorBaseNode.BatchEventWrapper> list = new ArrayList<>();
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			list.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));

			assertDoesNotThrow(() -> hazelcastMergeNode.loggerBatchUpdateCache(list));
		}
	}

	@Nested
	@DisplayName("Method lookupAndWrapMergeInfoConcurrent test")
	class lookupAndWrapMergeInfoConcurrentTest {

		private ExecutorService lookupThreadPool;

		@BeforeEach
		void setUp() {
			hazelcastMergeNode = spy(hazelcastMergeNode);
			lookupThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
					r -> {
						Thread thread = new Thread(r);
						thread.setName("Merge-Processor-Lookup-Thread-" + thread.getId());
						return thread;
					});
			ReflectionTestUtils.setField(hazelcastMergeNode, "lookupThreadPool", lookupThreadPool);
			HazelcastMergeNode.BatchProcessMetrics batchProcessMetrics = mock(HazelcastMergeNode.BatchProcessMetrics.class);
			ReflectionTestUtils.setField(hazelcastMergeNode, "batchProcessMetrics", batchProcessMetrics);
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			MergeInfo mergeInfo = new MergeInfo();
			doReturn(mergeInfo).when(hazelcastMergeNode).wrapMergeInfo(any(TapdataEvent.class));
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
			doReturn(mergeLookupResults).when(hazelcastMergeNode).lookup(any(TapdataEvent.class));

			CompletableFuture<Void> completableFuture = hazelcastMergeNode.lookupAndWrapMergeInfoConcurrent(tapdataEvent);

			assertNotNull(completableFuture);
			completableFuture.join();
			assertEquals(mergeLookupResults, mergeInfo.getMergeLookupResults());
		}

		@Test
		@DisplayName("test node logger debug enabled")
		void testNodeLoggerDebugEnabled() {
			ObsLogger nodeLogger = mock(ObsLogger.class);
			when(nodeLogger.isDebugEnabled()).thenReturn(true);
			ReflectionTestUtils.setField(hazelcastMergeNode, "nodeLogger", nodeLogger);
			TapdataEvent tapdataEvent = new TapdataEvent();
			MergeInfo mergeInfo = new MergeInfo();
			doReturn(mergeInfo).when(hazelcastMergeNode).wrapMergeInfo(any(TapdataEvent.class));
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
			doReturn(mergeLookupResults).when(hazelcastMergeNode).lookup(any(TapdataEvent.class));

			CompletableFuture<Void> completableFuture = hazelcastMergeNode.lookupAndWrapMergeInfoConcurrent(tapdataEvent);

			completableFuture.join();
			verify(nodeLogger, timeout(1)).debug(anyString(), any(Object[].class));
		}

		@Test
		void testLoopUpThrowException() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastMergeNode.lookup(tapdataEvent);
			});
			assertEquals(TaskMergeProcessorExCode_16.LOOK_UP_MISSING_FROM_NODE_ID, tapCodeException.getCode());
		}

		@AfterEach
		void tearDown() {
			lookupThreadPool.shutdownNow();
		}
	}

	@Nested
	class PutInCheckJoinKeyUpdateCacheMapAndWriteSignTest {
		HazelcastMergeNode mockHazelcastMergeNode;
		Map<String, ConstructIMap<Document>> iMapMap;
		String id = "sourceId";
		Map<String, Connections> sourceConnectionMap;

		@BeforeEach
		void init() {
			mockHazelcastMergeNode = mock(HazelcastMergeNode.class);
			ExternalStorageDto externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setType("Mongo");
			externalStorageDto.setName("Mongo External");
			ObsLogger obsLogger = mock(ObsLogger.class);
			Processor.Context context = mock(Processor.Context.class);
			iMapMap = new HashMap<>();
			sourceConnectionMap = new HashMap<>();
			sourceConnectionMap.put(id, new Connections());
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "jetContext", context);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "obsLogger", obsLogger);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "externalStorageDto", externalStorageDto);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", iMapMap);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "sourceConnectionMap", sourceConnectionMap);
			Node tableNode = mock(TableNode.class);
			when(mockHazelcastMergeNode.getPreNode(id)).thenReturn(tableNode);
			doCallRealMethod().when(mockHazelcastMergeNode).putInCheckJoinKeyUpdateCacheMapAndWriteSign(anyString(), anyString());
			when(mockHazelcastMergeNode.copyExternalStorage(anyInt())).thenReturn(externalStorageDto);
		}

		@DisplayName("test putInCheckJoinKeyUpdateCacheMapAndWriteSign throw exception INIT_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_WRITE_SIGN_FAILED")
		@SneakyThrows
		@Test
		void test1() {
			ConstructIMap constructIMap = mock(ConstructIMap.class);
			when(constructIMap.isEmpty()).thenReturn(true);
			when(constructIMap.getName()).thenReturn("cacheName");
			when(constructIMap.insert(anyString(), any())).thenThrow(new RuntimeException("insert Failed"));
			try (MockedStatic<HazelcastMergeNode> hazelcastMergeNodeMockedStatic = mockStatic(HazelcastMergeNode.class);) {
				hazelcastMergeNodeMockedStatic.when(() -> {
					HazelcastMergeNode.buildConstructIMap(any(), any(), any(), any());
				}).thenReturn(constructIMap);
				when(mockHazelcastMergeNode.checkJoinKeyIncludePK(anyString())).thenReturn(null);
				TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
					mockHazelcastMergeNode.putInCheckJoinKeyUpdateCacheMapAndWriteSign(id, "cacheName");
				});
				assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.INIT_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_WRITE_SIGN_FAILED);

			}

		}

		@SneakyThrows
		@Test
		void test_INIT_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_WRITE_SIGN_FAILED() {
			ConstructIMap constructIMap = mock(ConstructIMap.class);
			when(constructIMap.isEmpty()).thenReturn(true);
			when(constructIMap.getName()).thenReturn("cacheName");
			when(constructIMap.insert(anyString(), any())).thenThrow(new RuntimeException("insert Failed"));
			try (MockedStatic<HazelcastMergeNode> hazelcastMergeNodeMockedStatic = mockStatic(HazelcastMergeNode.class);) {
				hazelcastMergeNodeMockedStatic.when(() -> {
					HazelcastMergeNode.buildConstructIMap(any(), any(), any(), any());
				}).thenReturn(constructIMap);
				when(mockHazelcastMergeNode.isSourceHaveBefore(id)).thenReturn(true);
				when(mockHazelcastMergeNode.checkJoinKeyIncludePK(anyString())).thenReturn(null);
				TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
					mockHazelcastMergeNode.putInCheckJoinKeyUpdateCacheMapAndWriteSign(id, "cacheName");
				});
				assertEquals(TaskMergeProcessorExCode_16.INIT_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_WRITE_SIGN_FAILED, tapCodeException.getCode());
			}
		}
	}

	@Nested
	@DisplayName("Method doBatchCache test")
	class doBatchCacheTest {
		@BeforeEach
		void setUp() {
			hazelcastMergeNode = spy(hazelcastMergeNode);
			HazelcastMergeNode.BatchProcessMetrics batchProcessMetrics = mock(HazelcastMergeNode.BatchProcessMetrics.class);
			ReflectionTestUtils.setField(hazelcastMergeNode, "batchProcessMetrics", batchProcessMetrics);
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertInstanceOf(ArrayList.class, argument1);
				for (Object o : ((ArrayList<?>) argument1)) {
					assertInstanceOf(TapdataEvent.class, o);
				}
				return null;
			}).when(hazelcastMergeNode).cache(any(List.class));

			hazelcastMergeNode.doBatchCache(batchEventWrappers);

			verify(hazelcastMergeNode, times(1)).cache(any(List.class));
		}
	}

	@Nested
	class TestDeleteCache {
		HazelcastMergeNode mockHazelcastMergeNode;
		ConstructIMap<Document> constructIMap;
		MergeTableProperties mergeTableProperties;

		@BeforeEach
		void init() {
			mockHazelcastMergeNode = mock(HazelcastMergeNode.class);
			constructIMap = mock(ConstructIMap.class);
			ExternalStorageDto externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setType("Mongo");
			externalStorageDto.setName("Mongo ExteralStorageDto");
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "externalStorageDto", externalStorageDto);
			mergeTableProperties = mock(MergeTableProperties.class);
			when(mergeTableProperties.getId()).thenReturn("nodeId");
			Node tableNode = mock(TableNode.class);
			when(tableNode.getName()).thenReturn("nodeName");
			when(mockHazelcastMergeNode.getPreNode(anyString())).thenReturn(tableNode);
		}

		@DisplayName("test delete cache find error")
		@Test
		void test1() throws Exception {
			TapdataEvent tapdataEvent = new TapdataEvent();
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("joinKey", 1);
			TapDeleteRecordEvent deleteRecordEvent = TapDeleteRecordEvent.create();
			deleteRecordEvent.setBefore(before);
			tapdataEvent.setTapEvent(deleteRecordEvent);

			String joinValue = "1";
			when(mockHazelcastMergeNode.getJoinValueKeyBySource(any(), any(), any())).thenReturn(joinValue);
			String pkValue = "1";
			when(mockHazelcastMergeNode.getPkOrUniqueValueKey(anyMap(), any(MergeTableProperties.class), any())).thenReturn(pkValue);
			when(constructIMap.find(anyString())).thenThrow(new RuntimeException("find Failed"));
			doCallRealMethod().when(mockHazelcastMergeNode).encode(anyString());
			doCallRealMethod().when(mockHazelcastMergeNode).getBefore(any());
			doCallRealMethod().when(mockHazelcastMergeNode).deleteCache(any(), any(), any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				mockHazelcastMergeNode.deleteCache(tapdataEvent, mergeTableProperties, constructIMap);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.DELETE_CACHE_FIND_BY_JOIN_KEY_FAILED);
		}

		@DisplayName("test delete cache delete failed")
		@Test
		void test2() throws Exception {
			TapdataEvent tapdataEvent = new TapdataEvent();
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("joinKey", 1);
			TapDeleteRecordEvent deleteRecordEvent = TapDeleteRecordEvent.create();
			deleteRecordEvent.setBefore(before);
			tapdataEvent.setTapEvent(deleteRecordEvent);

			String joinValue = "1";
			when(mockHazelcastMergeNode.getJoinValueKeyBySource(any(), any(), any())).thenReturn(joinValue);
			String pkValue = "1";
			when(mockHazelcastMergeNode.getPkOrUniqueValueKey(anyMap(), any(MergeTableProperties.class), any())).thenReturn(pkValue);
			Document document = new Document();
			when(constructIMap.find(anyString())).thenReturn(document);
			when(constructIMap.delete(anyString())).thenThrow(new RuntimeException("delete failed"));
			doCallRealMethod().when(mockHazelcastMergeNode).encode(anyString());
			doCallRealMethod().when(mockHazelcastMergeNode).getBefore(any());
			doCallRealMethod().when(mockHazelcastMergeNode).deleteCache(any(), any(), any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				mockHazelcastMergeNode.deleteCache(tapdataEvent, mergeTableProperties, constructIMap);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.DELETE_CACHE_FAILED);
		}
	}

	@Nested
	class TestGetJoinValueKeyBySource {
		HazelcastMergeNode mockHazelcastMergeNode;
		ConstructIMap<Document> constructIMap;
		MergeTableProperties mergeTableProperties;

		@BeforeEach
		void beforeInit() {
			constructIMap = mock(ConstructIMap.class);
			mockHazelcastMergeNode = mock(HazelcastMergeNode.class);
			mergeTableProperties = mock(MergeTableProperties.class);
			when(mergeTableProperties.getId()).thenReturn("nodeId");
			Node tableNode = mock(TableNode.class);
			when(tableNode.getName()).thenReturn("nodeName");
			when(mockHazelcastMergeNode.getPreNode(anyString())).thenReturn(tableNode);
		}

		@DisplayName("test getJoinValueKeyBySource missing join key")
		@Test
		void test1() {
			Map<String, Object> after = new HashMap<>();
			ExternalStorageDto externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setType("Mongo");
			externalStorageDto.setName("Mongo ExteralStorageDto");
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "externalStorageDto", externalStorageDto);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			when(mergeTableProperties.getJoinKeys()).thenReturn(joinKeys);
			when(mockHazelcastMergeNode.getNode()).thenReturn(mock(Node.class));
			doCallRealMethod().when(mockHazelcastMergeNode).getJoinValueKeyBySource(after, mergeTableProperties, constructIMap);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				mockHazelcastMergeNode.getJoinValueKeyBySource(after, mergeTableProperties, constructIMap);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.MISSING_SOURCE_JOIN_KEY_CONFIG);
		}

		@DisplayName("test getJoinValueKeyBySource after not have join key")
		@Test
		void test2() {
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			ExternalStorageDto externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setType("Mongo");
			externalStorageDto.setName("Mongo ExteralStorageDto");
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "externalStorageDto", externalStorageDto);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			Map<String, String> sourceJoinKey = new HashMap<>();
			sourceJoinKey.put(HazelcastPreviewMergeNode.JoinKeyEnum.SOURCE.getKey(), "joinKey");
			joinKeys.add(sourceJoinKey);
			when(mergeTableProperties.getJoinKeys()).thenReturn(joinKeys);
			doCallRealMethod().when(mockHazelcastMergeNode).getJoinKeys(any(), any());
			doCallRealMethod().when(mockHazelcastMergeNode).getJoinValueKeyBySource(after, mergeTableProperties, constructIMap);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				mockHazelcastMergeNode.getJoinValueKeyBySource(after, mergeTableProperties, constructIMap);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.JOIN_KEY_VALUE_NOT_EXISTS);
		}
	}

	@Nested
	class TestGetPkOrUniqueValueKey {
		HazelcastMergeNode mockHazelcastMergeNode;
		ConstructIMap<Document> constructIMap;
		MergeTableProperties mergeTableProperties;
		Map<String, List<String>> sourcePkOrUniqueFieldMap;

		@BeforeEach
		void init() {
			constructIMap = mock(ConstructIMap.class);
			mockHazelcastMergeNode = mock(HazelcastMergeNode.class);
			mergeTableProperties = mock(MergeTableProperties.class);
			sourcePkOrUniqueFieldMap = new HashMap<>();
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "sourcePkOrUniqueFieldMap", sourcePkOrUniqueFieldMap);
		}

		@DisplayName("test getPkOrUniqueValueKey value not exist")
		@Test
		void test1() {
			List<String> pkList = new ArrayList<>();
			pkList.add("id");
			sourcePkOrUniqueFieldMap.put("sourceId", pkList);
			doCallRealMethod().when(mockHazelcastMergeNode).getPkOrUniqueValueKey(anyMap(), anyString(), any(ConstructIMap.class));
			Map<String, Object> after = new HashMap<>();
			after.put("cid", 1);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				mockHazelcastMergeNode.getPkOrUniqueValueKey(after, "sourceId", constructIMap);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.PK_OR_UNIQUE_VALUE_NOT_EXISTS);
		}
	}

	@Nested
	class getAndUpdateJoinKeyCacheClass {
		HazelcastMergeNode mockHazelcastMergeNode;
		ConstructIMap<Document> constructIMap;
		Map<String, ConstructIMap<Document>> checkJoinKeyUpdateCacheMap;

		@BeforeEach
		void init() {
			checkJoinKeyUpdateCacheMap = new HashMap<>();
			mockHazelcastMergeNode = mock(HazelcastMergeNode.class);
			constructIMap = mock(ConstructIMap.class);
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap", checkJoinKeyUpdateCacheMap);
			Node tableNode = mock(TableNode.class);
			when(tableNode.getName()).thenReturn("nodeName");
			when(mockHazelcastMergeNode.getPreNode(anyString())).thenReturn(tableNode);
			ExternalStorageDto externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setType("Mongo");
			externalStorageDto.setName("Mongo ExteralStorageDto");
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "externalStorageDto", externalStorageDto);
		}

		@DisplayName("test throw GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP ")
		@Test
		void test1() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("nodeId");
			doCallRealMethod().when(mockHazelcastMergeNode).getPreNodeId(any());
			doCallRealMethod().when(mockHazelcastMergeNode).getAndUpdateJoinKeyCache(any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				mockHazelcastMergeNode.getAndUpdateJoinKeyCache(tapdataEvent);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP);
		}

		@DisplayName("test throw GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_FIND_BY_PK_FAILED exception")
		@Test
		void test2() throws Exception {
			checkJoinKeyUpdateCacheMap.put("nodeId", constructIMap);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("nodeId");
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create();
			tapInsertRecordEvent.setAfter(after);
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			when(mockHazelcastMergeNode.getPkOrUniqueValueKey(anyMap(), anyString(), any(ConstructIMap.class))).thenReturn("1");
			when(constructIMap.find(anyString())).thenThrow(new RuntimeException("throw failed"));
			doCallRealMethod().when(mockHazelcastMergeNode).getPreNodeId(any());
			doCallRealMethod().when(mockHazelcastMergeNode).getAndUpdateJoinKeyCache(any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				mockHazelcastMergeNode.getAndUpdateJoinKeyCache(tapdataEvent);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_FIND_BY_PK_FAILED);
		}

		@DisplayName("test getAndUpdateJoinKeyCache throw GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED")
		@Test
		void test3() throws Exception {
			checkJoinKeyUpdateCacheMap.put("nodeId", constructIMap);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("nodeId");
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create();
			tapInsertRecordEvent.setAfter(after);
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			when(mockHazelcastMergeNode.getPkOrUniqueValueKey(anyMap(), anyString(), any(ConstructIMap.class))).thenReturn("1");
			Document document = new Document();
			when(constructIMap.find(anyString())).thenReturn(document);
			when(constructIMap.upsert(any(), any())).thenThrow(new RuntimeException("upsertFailed"));
			doCallRealMethod().when(mockHazelcastMergeNode).getPreNodeId(any());
			doCallRealMethod().when(mockHazelcastMergeNode).getAndUpdateJoinKeyCache(any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				mockHazelcastMergeNode.getAndUpdateJoinKeyCache(tapdataEvent);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED);
		}

		@DisplayName("test throw GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_FIND_BEFORE exception")
		@Test
		void test4() throws Exception {
			checkJoinKeyUpdateCacheMap.put("nodeId", constructIMap);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.addNodeId("nodeId");
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create();
			tapInsertRecordEvent.setAfter(after);
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			when(mockHazelcastMergeNode.getPkOrUniqueValueKey(anyMap(), anyString(), any(ConstructIMap.class))).thenReturn("1");
			when(constructIMap.find(anyString())).thenReturn(null);
			doCallRealMethod().when(mockHazelcastMergeNode).getPreNodeId(any());
			doCallRealMethod().when(mockHazelcastMergeNode).getAndUpdateJoinKeyCache(any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				mockHazelcastMergeNode.getAndUpdateJoinKeyCache(tapdataEvent);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_FIND_BEFORE);
		}
	}

	@Nested
	@DisplayName("Method recursiveLookup test")
	class recursiveLookupTest {

		private ObsLogger nodeLogger;

		@BeforeEach
		void setUp() {
			hazelcastMergeNode = spy(hazelcastMergeNode);
			nodeLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(hazelcastMergeNode, "nodeLogger", nodeLogger);
		}

		@Test
		@SneakyThrows
		@DisplayName("test update write lookup and return only one row")
		void testUpdateWriteLookupOnlyOneRow() {
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			MergeTableProperties mergeTableProperties1 = new MergeTableProperties();
			List<MergeTableProperties> mergeTablePropertiesList = new ArrayList<>();
			mergeTablePropertiesList.add(mergeTableProperties);
			mergeTableProperties1.setChildren(mergeTablePropertiesList);
			mergeTableProperties.setId("1");
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setTargetPath("test");
			Map<String, String> joinKeyMap = new HashMap<>();
			joinKeyMap.put("source", "id");
			joinKeyMap.put("target", "id");
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(joinKeyMap);
			mergeTableProperties.setJoinKeys(joinKeys);
			Map<String, Object> data = new HashMap<>();
			data.put("id", 1);
			ConstructIMap<Document> constructIMap = mock(ConstructIMap.class);
			when(constructIMap.getName()).thenReturn("test");
			Document findData = new Document("1", new Document("id", 1));
			when(constructIMap.find(anyString())).thenReturn(findData);
			Node preNode = mock(TableNode.class);
			when(((TableNode) preNode).getTableName()).thenReturn("test");
			doReturn(preNode).when(hazelcastMergeNode).getPreNode("1");
			doReturn(constructIMap).when(hazelcastMergeNode).getHazelcastConstruct(anyString());
			TapTable tapTable = new TapTable("test");
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get("test")).thenReturn(tapTable);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);

			List<MergeLookupResult> mergeLookupResults = hazelcastMergeNode.recursiveLookup(mergeTableProperties1, data, true);

			verify(nodeLogger, times(0)).warn(eq("Update write merge lookup, find more than one row, lookup table: {}, join key value: {}, will use first row: {}"), any(Object[].class));
		}

		@Test
		@SneakyThrows
		@DisplayName("test update write lookup and return only one row")
		void testUpdateWriteLookupOnlyOneRowThrowError() {
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			MergeTableProperties mergeTableProperties1 = new MergeTableProperties();
			List<MergeTableProperties> mergeTablePropertiesList = new ArrayList<>();
			mergeTablePropertiesList.add(mergeTableProperties);
			mergeTableProperties1.setChildren(mergeTablePropertiesList);
			mergeTableProperties1.setId("2");
			mergeTableProperties.setId("1");
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setTargetPath("test");
			Map<String, String> joinKeyMap = new HashMap<>();
			joinKeyMap.put("source", "id");
			joinKeyMap.put("target", "id");
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(joinKeyMap);
			mergeTableProperties.setJoinKeys(joinKeys);
			Map<String, Object> data = new HashMap<>();
			data.put("id", 1);
			ConstructIMap<Document> constructIMap = mock(ConstructIMap.class);
			when(constructIMap.getName()).thenReturn("test");
			when(constructIMap.find(anyString())).thenThrow(new RuntimeException("connect failed"));
			Node preNode = mock(TableNode.class);
			when(((TableNode) preNode).getTableName()).thenReturn("test");
			Node fatherPreNode = mock(TableNode.class);
			when(((TableNode) preNode).getTableName()).thenReturn("fathertest");
			ExternalStorageDto externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setType("Mongo");
			externalStorageDto.setName("externalName");
			ReflectionTestUtils.setField(hazelcastMergeNode, "externalStorageDto", externalStorageDto);
			doReturn(preNode).when(hazelcastMergeNode).getPreNode("1");
			doReturn(fatherPreNode).when(hazelcastMergeNode).getPreNode("2");
			doReturn(constructIMap).when(hazelcastMergeNode).getHazelcastConstruct(anyString());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastMergeNode.recursiveLookup(mergeTableProperties1, data, true);
			});
			assertEquals(TaskMergeProcessorExCode_16.LOOK_UP_FIND_BY_JOIN_KEY_FAILED, tapCodeException.getCode());
		}

		@Test
		@SneakyThrows
		@DisplayName("test update write lookup and return more than one row")
		void testUpdateWriteLookupMoreThanOneRow() {
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			MergeTableProperties mergeTableProperties1 = new MergeTableProperties();
			List<MergeTableProperties> mergeTablePropertiesList = new ArrayList<>();
			mergeTablePropertiesList.add(mergeTableProperties);
			mergeTableProperties1.setChildren(mergeTablePropertiesList);
			mergeTableProperties.setId("1");
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setTargetPath("test");
			Map<String, String> joinKeyMap = new HashMap<>();
			joinKeyMap.put("source", "id");
			joinKeyMap.put("target", "id");
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(joinKeyMap);
			mergeTableProperties.setJoinKeys(joinKeys);
			Map<String, Object> data = new HashMap<>();
			data.put("id", 1);
			ConstructIMap<Document> constructIMap = mock(ConstructIMap.class);
			when(constructIMap.getName()).thenReturn("test");
			Document findData = new Document("1", new Document("id", 1)).append("2", new Document("id", 2));
			when(constructIMap.find(anyString())).thenReturn(findData);
			Node preNode = mock(TableNode.class);
			when(((TableNode) preNode).getTableName()).thenReturn("test");
			doReturn(preNode).when(hazelcastMergeNode).getPreNode("1");
			doReturn(constructIMap).when(hazelcastMergeNode).getHazelcastConstruct(anyString());
			TapTable tapTable = new TapTable("test");
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get("test")).thenReturn(tapTable);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);

			List<MergeLookupResult> mergeLookupResults = hazelcastMergeNode.recursiveLookup(mergeTableProperties1, data, true);

			assertNotNull(mergeLookupResults);
			assertEquals(1, mergeLookupResults.size());
			assertNotNull(mergeLookupResults.get(0));
			MergeLookupResult mergeLookupResult = mergeLookupResults.get(0);
			assertNotNull(mergeLookupResult.getProperty());
			assertNotNull(mergeLookupResult.getData());
			Map<String, Object> actualData = mergeLookupResult.getData();
			assertEquals(1, actualData.size());
			assertEquals(1, actualData.get("id"));
			assertTrue(mergeLookupResult.isDataExists());
			assertEquals(tapTable, mergeLookupResult.getTapTable());
			assertNull(mergeLookupResult.getSharedJoinKeys());
			assertTrue(mergeLookupResult.getMergeLookupResults().isEmpty());
			verify(nodeLogger, times(1)).warn(eq("Update write merge lookup, find more than one row, lookup table: {}, join key value: {}, will use first row: {}"), any(Object[].class));
		}
	}

	@Nested
	class InitSourcePkOrUniqueFieldMapTest {
		ProcessorBaseContext processorBaseContextTest;
		private Map<String, List<String>> sourcePkOrUniqueFieldMap;

		@BeforeEach
		void setUp() {
			sourcePkOrUniqueFieldMap = new HashMap<>();
			processorBaseContextTest = mock(ProcessorBaseContext.class);
			ReflectionTestUtils.setField(hazelcastMergeNode, "processorBaseContext", processorBaseContextTest);
			ReflectionTestUtils.setField(hazelcastMergeNode, "sourcePkOrUniqueFieldMap", sourcePkOrUniqueFieldMap);
		}


		@Test
		void testNodeIsDisabled() {
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setChildren(new ArrayList<>());
			List<MergeTableProperties> mergeTablePropertiesList = new ArrayList<>();
			mergeTablePropertiesList.add(mergeTableProperties);
			List<Node> nodes = new ArrayList<>();
			Map<String, Object> attrs = new HashMap<>();
			attrs.put("disabled", true);
			TableNode node = new TableNode();
			node.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			node.setAttrs(attrs);
			nodes.add(node);
			when(processorBaseContextTest.getNodes()).thenReturn(nodes);
			hazelcastMergeNode.initSourcePkOrUniqueFieldMap(mergeTablePropertiesList);
			Assertions.assertEquals(0, sourcePkOrUniqueFieldMap.size());
		}

		@Test
		void testNodeIsNotDisabled() {
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setChildren(new ArrayList<>());
			List<MergeTableProperties> mergeTablePropertiesList = new ArrayList<>();
			mergeTablePropertiesList.add(mergeTableProperties);
			List<Node> nodes = new ArrayList<>();
			Map<String, Object> attrs = new HashMap<>();
			TableNode node = new TableNode();
			node.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			node.setTableName("test");
			node.setAttrs(attrs);
			nodes.add(node);
			when(processorBaseContextTest.getNodes()).thenReturn(nodes);
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(processorBaseContextTest.getTapTableMap()).thenReturn(tapTableMap);
			TapTable tapTable = mock(TapTable.class);
			when(tapTableMap.get(any())).thenReturn(tapTable);
			when(tapTable.primaryKeys(true)).thenReturn(Arrays.asList("id"));
			hazelcastMergeNode.initSourcePkOrUniqueFieldMap(mergeTablePropertiesList);
			Assertions.assertEquals(1, sourcePkOrUniqueFieldMap.size());
		}

		@Test
		void testNoPrimaryKeyThrowException() {
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setChildren(new ArrayList<>());
			List<MergeTableProperties> mergeTablePropertiesList = new ArrayList<>();
			mergeTablePropertiesList.add(mergeTableProperties);
			List<Node> nodes = new ArrayList<>();
			Map<String, Object> attrs = new HashMap<>();
			TableNode node = new TableNode();
			node.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			node.setTableName("test");
			node.setAttrs(attrs);
			nodes.add(node);
			when(processorBaseContextTest.getNodes()).thenReturn(nodes);
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(processorBaseContextTest.getTapTableMap()).thenReturn(tapTableMap);
			TapTable tapTable = mock(TapTable.class);
			when(tapTableMap.get(any())).thenReturn(tapTable);
			when(tapTable.primaryKeys(true)).thenReturn(null);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastMergeNode.initSourcePkOrUniqueFieldMap(mergeTablePropertiesList);
			});
			assertEquals(TaskMergeProcessorExCode_16.TAP_MERGE_TABLE_NO_PRIMARY_KEY, tapCodeException.getCode());
		}

		@Test
		void test1() {
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			mergeTableProperties.setChildren(new ArrayList<>());
			List<MergeTableProperties> mergeTablePropertiesList = new ArrayList<>();
			mergeTablePropertiesList.add(mergeTableProperties);
			List<Node> nodes = new ArrayList<>();
			Map<String, Object> attrs = new HashMap<>();
			TableNode node = new TableNode();
			node.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			node.setTableName("test");
			node.setAttrs(attrs);
			nodes.add(node);
			when(processorBaseContextTest.getNodes()).thenReturn(nodes);
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(processorBaseContextTest.getTapTableMap()).thenReturn(tapTableMap);
			TapTable tapTable = mock(TapTable.class);
			when(tapTableMap.get(any())).thenReturn(tapTable);
			when(tapTable.primaryKeys(true)).thenReturn(Arrays.asList("id"));
			try {
				hazelcastMergeNode.initSourcePkOrUniqueFieldMap(mergeTablePropertiesList);
			} catch (TapCodeException e) {
				assertEquals(TaskMergeProcessorExCode_16.TAP_MERGE_TABLE_NO_ARRAY_KEY, e.getCode());
			}

		}

		@Test
		@DisplayName("merge type: update write, array keys: id, don't have any primary keys")
		void test2() {
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setChildren(new ArrayList<>());
			mergeTableProperties.setArrayKeys(Arrays.asList("id"));
			List<MergeTableProperties> mergeTablePropertiesList = new ArrayList<>();
			mergeTablePropertiesList.add(mergeTableProperties);
			List<Node> nodes = new ArrayList<>();
			Map<String, Object> attrs = new HashMap<>();
			TableNode node = new TableNode();
			node.setId("2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4");
			node.setTableName("test");
			node.setAttrs(attrs);
			nodes.add(node);
			when(processorBaseContextTest.getNodes()).thenReturn(nodes);
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(processorBaseContextTest.getTapTableMap()).thenReturn(tapTableMap);
			TapTable tapTable = mock(TapTable.class);
			when(tapTableMap.get(any())).thenReturn(tapTable);
			when(tapTable.primaryKeys(true)).thenReturn(null);
			hazelcastMergeNode.initSourcePkOrUniqueFieldMap(mergeTablePropertiesList);
			Assertions.assertEquals(1, sourcePkOrUniqueFieldMap.size());
		}

	}

	@Nested
	@DisplayName("Method checkBuildConstructIMap test")
	class checkBuildConstructIMapTest {
		@Test
		void test_createNewHashConstructIMap() {
			try (MockedStatic<ExternalStorageUtil> utilMockedStatic = mockStatic(ExternalStorageUtil.class);
				 MockedStatic<PersistenceStorage> persistenceStorageMockedStatic = mockStatic(PersistenceStorage.class)) {
				PersistenceStorage persistenceStorage = mock(PersistenceStorage.class);
				persistenceStorageMockedStatic.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
				when(persistenceStorage.isEmpty(any(), any())).thenReturn(false);
				HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
				IMap map = mock(IMap.class);
				String cacheNameHash = String.valueOf("test".hashCode());
				when(map.getName()).thenReturn(cacheNameHash);
				when(hazelcastInstance.getMap(cacheNameHash)).thenReturn(map);
				utilMockedStatic.when(() -> ExternalStorageUtil.initHZMapStorage(any(), any(), any(), any())).thenAnswer(invocation -> null);
				ConstructIMap<Document> result = HazelcastMergeNode.checkBuildConstructIMap(hazelcastInstance, "test", "test", mock(ExternalStorageDto.class));
				Assertions.assertEquals(cacheNameHash, result.getName());
			}
		}

		@Test
		void test_createNewHashConstructIMapError() {
			try (MockedStatic<ExternalStorageUtil> utilMockedStatic = mockStatic(ExternalStorageUtil.class)) {
				HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
				utilMockedStatic.when(() -> ExternalStorageUtil.initHZMapStorage(any(), any(), any(), any())).thenThrow(RuntimeException.class);
				Assertions.assertThrows(TapCodeException.class, () -> HazelcastMergeNode.checkBuildConstructIMap(hazelcastInstance, "test", "test", mock(ExternalStorageDto.class)));
			}
		}

		@Test
		void test_createOldConstructIMapError() {
			try (MockedStatic<ExternalStorageUtil> utilMockedStatic = mockStatic(ExternalStorageUtil.class);
				 MockedStatic<PersistenceStorage> persistenceStorageMockedStatic = mockStatic(PersistenceStorage.class)) {
				PersistenceStorage persistenceStorage = mock(PersistenceStorage.class);
				persistenceStorageMockedStatic.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
				HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
				IMap map = mock(IMap.class);
				String cacheNameHash = String.valueOf("test".hashCode());
				when(persistenceStorage.isEmpty(ConstructType.IMAP, cacheNameHash)).thenReturn(true);
				when(map.getName()).thenReturn(cacheNameHash);
				when(hazelcastInstance.getMap(cacheNameHash)).thenReturn(map);
				utilMockedStatic.when(() -> ExternalStorageUtil.initHZMapStorage(any(), any(), any(), any())).thenAnswer(invocation -> null).thenThrow(RuntimeException.class);
				ConstructIMap<Document> result = HazelcastMergeNode.checkBuildConstructIMap(hazelcastInstance, "test", "test", mock(ExternalStorageDto.class));
				Assertions.assertEquals(cacheNameHash, result.getName());
			}
		}

		@Test
		void test_oldConstructIMapIsNotEmpty() {
			try (MockedStatic<ExternalStorageUtil> utilMockedStatic = mockStatic(ExternalStorageUtil.class);
				 MockedStatic<PersistenceStorage> persistenceStorageMockedStatic = mockStatic(PersistenceStorage.class)) {
				PersistenceStorage persistenceStorage = mock(PersistenceStorage.class);
				persistenceStorageMockedStatic.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
				HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
				String cacheNameHash = String.valueOf("test".hashCode());
				when(persistenceStorage.isEmpty(ConstructType.IMAP, cacheNameHash)).thenReturn(true);
				when(persistenceStorage.isEmpty(ConstructType.IMAP, "test")).thenReturn(false);
				IMap map = mock(IMap.class);
				when(map.getName()).thenReturn(cacheNameHash);
				when(hazelcastInstance.getMap(cacheNameHash)).thenReturn(map);
				IMap map2 = mock(IMap.class);
				when(map2.getName()).thenReturn("test");
				when(hazelcastInstance.getMap("test")).thenReturn(map2);
				utilMockedStatic.when(() -> ExternalStorageUtil.initHZMapStorage(any(), any(), any(), any())).thenAnswer(invocation -> null);
				ConstructIMap<Document> result = HazelcastMergeNode.checkBuildConstructIMap(hazelcastInstance, "test", "test", mock(ExternalStorageDto.class));
				Assertions.assertEquals("test", result.getName());
			}
		}

		@Test
		void test_oldConstructIMapIsEmpty() {
			try (MockedStatic<ExternalStorageUtil> utilMockedStatic = mockStatic(ExternalStorageUtil.class);
				 MockedStatic<PersistenceStorage> persistenceStorageMockedStatic = mockStatic(PersistenceStorage.class)) {
				PersistenceStorage persistenceStorage = mock(PersistenceStorage.class);
				persistenceStorageMockedStatic.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
				HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
				String cacheNameHash = String.valueOf("test".hashCode());
				when(persistenceStorage.isEmpty(ConstructType.IMAP, cacheNameHash)).thenReturn(true);
				when(persistenceStorage.isEmpty(ConstructType.IMAP, "test")).thenReturn(true);
				IMap map = mock(IMap.class);
				when(map.getName()).thenReturn(cacheNameHash);
				when(hazelcastInstance.getMap(cacheNameHash)).thenReturn(map);
				IMap map2 = mock(IMap.class);
				when(map2.getName()).thenReturn("test");
				when(hazelcastInstance.getMap("test")).thenReturn(map2);
				utilMockedStatic.when(() -> ExternalStorageUtil.initHZMapStorage(any(), any(), any(), any())).thenAnswer(invocation -> null);
				ConstructIMap<Document> result = HazelcastMergeNode.checkBuildConstructIMap(hazelcastInstance, "test", "test", mock(ExternalStorageDto.class));
				Assertions.assertEquals(cacheNameHash, result.getName());
			}
		}

	}

	@Nested
	class CopyExternalStorageTest {
		@Test
		void test() {
			ExternalStorageDto externalStorageDto = new ExternalStorageDto();
			ExternalStorageDto result = HazelcastMergeNode.copyExternalStorage(externalStorageDto, 1);
			Assertions.assertNull(result.getTable());
			Assertions.assertEquals(1, result.getInMemSize());
			Assertions.assertEquals(10, result.getWriteDelaySeconds());
			Assertions.assertEquals(0, result.getTtlDay());
		}

	}

	@Nested
	class ClearCacheTest {
		@Test
		void testDass() {
			try (MockedStatic<ExternalStorageUtil> externalStorageUtil = mockStatic(ExternalStorageUtil.class);
				 MockedStatic<HazelcastUtil> hazelcastUtil = mockStatic(HazelcastUtil.class)) {
				externalStorageUtil.when(() -> ExternalStorageUtil.getExternalStorage(any())).thenAnswer(invocation -> {
					Node<?> node = invocation.getArgument(0);
					Assertions.assertEquals(mergeTableNode, node);
					return new ExternalStorageDto();
				});
				hazelcastUtil.when(() -> HazelcastUtil.getInstance()).thenReturn(mock(HazelcastInstance.class));
				HazelcastMergeNode.clearCache(mergeTableNode);
			}
		}

		@Test
		void testCloud() {
			try (MockedStatic<ExternalStorageUtil> externalStorageUtil = mockStatic(ExternalStorageUtil.class);
				 MockedStatic<HazelcastUtil> hazelcastUtil = mockStatic(HazelcastUtil.class)) {
				List<Node> nodes = new ArrayList<>();
				List<Edge> edges = new ArrayList<>();
				externalStorageUtil.when(() -> ExternalStorageUtil.getTargetNodeExternalStorage(any(), any(), any(), anyList())).thenAnswer(invocation -> {
					Node<?> node = invocation.getArgument(0);
					Assertions.assertEquals(mergeTableNode, node);
					return new ExternalStorageDto();
				});
				hazelcastUtil.when(() -> HazelcastUtil.getInstance()).thenReturn(mock(HazelcastInstance.class));
				HazelcastMergeNode.clearCache(mergeTableNode, nodes, edges);
			}
		}
	}

	@DisplayName("测试错误码，表名不能为空")
	@Test
	void test2() {
		try {
			TableNode tableNode = new TableNode();
			hazelcastMergeNode.getTableName(tableNode);
		} catch (TapCodeException e) {
			assertEquals(TaskMergeProcessorExCode_16.TABLE_NAME_CANNOT_BE_BLANK, e.getCode());
		}
	}

	@DisplayName("测试错误码，连接ID不能为空")
	@Test
	void test3() {
		try {
			TableNode tableNode = new TableNode();
			hazelcastMergeNode.getConnectionId(tableNode);
		} catch (TapCodeException e) {
			assertEquals(TaskMergeProcessorExCode_16.CONNECTION_ID_CANNOT_BE_BLANK, e.getCode());
		}
	}

	@Test
	void test4() {
		MergeTableProperties mergeTableProperties = new MergeTableProperties();
		mergeTableProperties.setId("sourceTableId");
		List<MergeTableProperties> mergeTablePropertiesList = new ArrayList<>();
		mergeTablePropertiesList.add(mergeTableProperties);
		try {
			hazelcastMergeNode.initSourcePkOrUniqueFieldMap(mergeTablePropertiesList);
		} catch (TapCodeException e) {
			assertEquals(TaskMergeProcessorExCode_16.TAP_MERGE_TABLE_NODE_NOT_FOUND, e.getCode());
		}

	}

	@Test
	void analyzeChildrenReferenceExTest() {
		Map<String, MergeTableProperties> mergeTablePropertiesMap = new HashMap<>();
		ReflectionTestUtils.setField(hazelcastMergeNode, "mergeTablePropertiesMap", mergeTablePropertiesMap);
		try {
			hazelcastMergeNode.analyzeChildrenReference("123");
		} catch (TapCodeException e) {
			assert e.getCode() == TaskMergeProcessorExCode_16.ANALYZE_CHILD_REFERENCE_FAILED_CANT_GET_MERGE_TABLE_PROPERTIES_BY_ID;
		}
	}

	@Test
	void getPreNodeExTest() {
		Map<String, Node<?>> preNodeMap = new ConcurrentHashMap<>();
		ProcessorBaseContext processorBaseContext = mock(ProcessorBaseContext.class);
		ReflectionTestUtils.setField(hazelcastMergeNode, "preNodeMap", preNodeMap);
		ReflectionTestUtils.setField(hazelcastMergeNode, "processorBaseContext", processorBaseContext);
		when(processorBaseContext.getNodes()).thenReturn(new ArrayList<>());
		try {
			hazelcastMergeNode.getPreNode("123");
		} catch (TapCodeException e) {
			assert e.getCode() == TaskMergeProcessorExCode_16.CANNOT_GET_PRENODE_BY_ID;
		}
	}

	@Nested
	class deleteOrUpdateMergeCacheTest {
		Document beforeDoc;
		ConstructIMap<Document> lookupCache;
		String encodeJoinKey;
		String beforeJoinValueKeyBySource;

		@BeforeEach
		void beforeEach() {
			lookupCache = mock(ConstructIMap.class);
			encodeJoinKey = "123";
			beforeJoinValueKeyBySource = "123";
		}

		@Test
		@SneakyThrows
		@DisplayName("test for REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_DELETE_CACHE_ERROR")
		void test1() {
			beforeDoc = new Document("_ts", 123456789L);
			when(lookupCache.delete(encodeJoinKey)).thenThrow(Exception.class);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastMergeNode.deleteOrUpdateMergeCache(beforeDoc, lookupCache, encodeJoinKey, beforeJoinValueKeyBySource);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_DELETE_CACHE_ERROR);
		}

		@Test
		@SneakyThrows
		@DisplayName("test for REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_UPDATE_CACHE_ERROR")
		void test2() {
			beforeDoc = new Document("test", 1);
			when(lookupCache.upsert(encodeJoinKey, beforeDoc)).thenThrow(Exception.class);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastMergeNode.deleteOrUpdateMergeCache(beforeDoc, lookupCache, encodeJoinKey, beforeJoinValueKeyBySource);
			});
			assertEquals(tapCodeException.getCode(), TaskMergeProcessorExCode_16.REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_UPDATE_CACHE_ERROR);
		}
	}

	@Nested
	class removeMergeCacheIfUpdateJoinKeyTest {
		TapdataEvent tapdataEvent;
		Map<String, Object> before;
		Map<String, MergeTableProperties> mergeTablePropertiesMap;
		MergeTableProperties mergeTableProperties;
		Map<String, List<String>> sourcePkOrUniqueFieldMap;

		@BeforeEach
		void beforeEach() {
			hazelcastMergeNode = spy(hazelcastMergeNode);
			tapdataEvent = mock(TapdataEvent.class);
			before = new HashMap<>();
			mergeTablePropertiesMap = new HashMap<>();
			mergeTableProperties = mock(MergeTableProperties.class);
			mergeTablePropertiesMap.put("123", mergeTableProperties);
			ReflectionTestUtils.setField(hazelcastMergeNode, "mergeTablePropertiesMap", mergeTablePropertiesMap);
			sourcePkOrUniqueFieldMap = new HashMap<>();
			ReflectionTestUtils.setField(hazelcastMergeNode, "sourcePkOrUniqueFieldMap", sourcePkOrUniqueFieldMap);

		}

		@Test
		@SneakyThrows
		void test_REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_FIND_CACHE_ERROR() {
			List<String> nodeIds = new ArrayList<>();
			nodeIds.add("123");
			ConstructIMap<Document> lookupCache = mock(ConstructIMap.class);
			doReturn(lookupCache).when(hazelcastMergeNode).getHazelcastConstruct(anyString());
			when(tapdataEvent.getNodeIds()).thenReturn(nodeIds);
			doReturn("1234").when(hazelcastMergeNode).getJoinValueKeyBySource(before, mergeTableProperties, lookupCache);
			doReturn("5678").when(hazelcastMergeNode).getJoinValueKeyBySource(null, mergeTableProperties, lookupCache);
			when(lookupCache.find(anyString())).thenThrow(RuntimeException.class);
			TapCodeException exception = assertThrows(TapCodeException.class, () -> hazelcastMergeNode.removeMergeCacheIfUpdateJoinKey(tapdataEvent, before));
			assertEquals(TaskMergeProcessorExCode_16.REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_FIND_CACHE_ERROR, exception.getCode());
		}
	}

	@Nested
	@DisplayName("Method mergeLookupData test")
	class mergeLookupDataTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			Map<String, Object> map1 = new HashMap<>();
			map1.put("f1", 1);
			map1.put("f2", 2);
			Map<String, Object> map2 = new HashMap<>();
			map2.put("f3", 3);
			map2.put("f4", 4);
			map2.put("f1", 5);
			Map<String, Object> result = hazelcastMergeNode.mergeLookupData(map1, map2);
			assertEquals(4, result.size());
			assertEquals(1, result.get("f1"));
		}

		@Test
		@DisplayName("test input parameters are empty")
		void test2() {
			Map<String, Object> map1 = new HashMap<>();
			Map<String, Object> map2 = new HashMap<>();
			map2.put("f2", 3);
			Map<String, Object> result = hazelcastMergeNode.mergeLookupData(map1, map2);
			assertSame(map1, result);

			map1.put("f1", 1);
			map2.clear();
			result = hazelcastMergeNode.mergeLookupData(map1, map2);
			assertSame(map1, result);

			map2.put("f2", 2);
			result = hazelcastMergeNode.mergeLookupData(null, map2);
			assertNull(result);

			result = hazelcastMergeNode.mergeLookupData(map1, null);
			assertSame(map1, result);
		}
	}
}

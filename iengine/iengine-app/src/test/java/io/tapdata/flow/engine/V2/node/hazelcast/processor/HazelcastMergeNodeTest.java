package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.AppType;
import com.tapdata.entity.Connections;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.error.TaskMergeProcessorExCode_16;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.apache.commons.collections4.MapUtils;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.time.Instant;
import java.util.*;

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
					MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class);
					MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class);
					MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = mockStatic(PDKIntegration.class)
			) {
				appTypeMockedStatic.when(AppType::init).thenReturn(appType);
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
					MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class);
					MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class);
					MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = mockStatic(PDKIntegration.class)
			) {
				appTypeMockedStatic.when(AppType::init).thenReturn(appType);
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
			when(tapdataEvent.getTapEvent()).thenReturn(tapInsertRecordEvent);
			constructIMap = mock(ConstructIMap.class);
			mergeTableProperties = mock(MergeTableProperties.class);
			sourcePkOrUniqueFieldMap = new HashMap<>();
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
			assertInstanceOf(Date.class, after.get("create_time"));
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
			assertEquals(2, actualMap.size());
			assertTrue(actualMap.containsKey("2"));
			assertTrue(actualMap.containsKey("4"));
			Object actualValue = actualMap.get("2");
			assertNotNull(actualValue);
			assertInstanceOf(HashSet.class, actualValue);
			assertEquals(3, ((HashSet) actualValue).size());
			assertTrue(((HashSet) actualValue).contains("city_id"));
			assertTrue(((HashSet) actualValue).contains("name"));
			assertTrue(((HashSet) actualValue).contains("xxx_id"));
			actualValue = actualMap.get("4");
			assertNotNull(actualValue);
			assertInstanceOf(HashSet.class, actualValue);
			assertEquals(1, ((HashSet) actualValue).size());
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
			doReturn(constructIMap).when(mockHazelcastMergeNode).buildConstructIMap(eq(hazelcastInstance), anyString(), anyString(), any(ExternalStorageDto.class));
			ReflectionTestUtils.setField(mockHazelcastMergeNode, "obsLogger", mockObsLogger);

			mockHazelcastMergeNode.initFirstLevelIds();
			mockHazelcastMergeNode.initMergeTableProperties();
			mockHazelcastMergeNode.initMergeTablePropertyReferenceMap();
		}

		@Test
		@SneakyThrows
		@DisplayName("main process test")
		void testMainProcess() {
			mockHazelcastMergeNode.initCheckJoinKeyUpdateCacheMap();

			Object checkJoinKeyUpdateCacheMapObj = ReflectionTestUtils.getField(mockHazelcastMergeNode, "checkJoinKeyUpdateCacheMap");
			assertInstanceOf(HashMap.class, checkJoinKeyUpdateCacheMapObj);
			HashMap checkJoinKeyUpdateCacheMap = (HashMap) checkJoinKeyUpdateCacheMapObj;
			assertEquals(1, checkJoinKeyUpdateCacheMap.size());
			assertTrue(checkJoinKeyUpdateCacheMap.containsKey("2"));
			Object constructIMapObj = checkJoinKeyUpdateCacheMap.get("2");
			assertInstanceOf(ConstructIMap.class, constructIMapObj);
			assertEquals(constructIMap, constructIMapObj);
		}

		@Test
		@DisplayName("when join key include pk")
		void whenJoinKeyIncludePK() {
			TapTable b = tapTableMap.get("b");
			b.getNameFieldMap().get("b_id").primaryKeyPos(1);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> mockHazelcastMergeNode.initCheckJoinKeyUpdateCacheMap());
			assertEquals(TaskMergeProcessorExCode_16.BUILD_CHECK_UPDATE_JOIN_KEY_CACHE_FAILED_JOIN_KEY_INCLUDE_PK, tapCodeException.getCode());
			assertNotNull(tapCodeException.getMessage());
		}
	}

	@Nested
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
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			HazelcastMergeNode.EnableUpdateJoinKey enableUpdateJoinKey = new HazelcastMergeNode.EnableUpdateJoinKey();
			enableUpdateJoinKey.enableParent();
			doReturn("1").when(hazelcastMergeNode).getPreNodeId(tapdataEvent);
			Map<String, HazelcastMergeNode.EnableUpdateJoinKey> enableUpdateJoinKeyMap = new HashMap<>();
			enableUpdateJoinKeyMap.put("1", enableUpdateJoinKey);
			ReflectionTestUtils.setField(hazelcastMergeNode, "enableUpdateJoinKeyMap", enableUpdateJoinKeyMap);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKeyIfNeed(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKeyIfNeed(batchEventWrappers);
			verify(hazelcastMergeNode, times(1)).handleUpdateJoinKeyIfNeed(tapdataEvent);
		}

		@Test
		@DisplayName("when enable children update join key")
		void whenEnableChildrenUpdateJoinKey() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			TapdataEvent tapdataEvent = new TapdataEvent();
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			HazelcastMergeNode.EnableUpdateJoinKey enableUpdateJoinKey = new HazelcastMergeNode.EnableUpdateJoinKey();
			enableUpdateJoinKey.enableChildren();
			doReturn("1").when(hazelcastMergeNode).getPreNodeId(tapdataEvent);
			Map<String, HazelcastMergeNode.EnableUpdateJoinKey> enableUpdateJoinKeyMap = new HashMap<>();
			enableUpdateJoinKeyMap.put("1", enableUpdateJoinKey);
			ReflectionTestUtils.setField(hazelcastMergeNode, "enableUpdateJoinKeyMap", enableUpdateJoinKeyMap);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKeyIfNeed(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKeyIfNeed(batchEventWrappers);
			verify(hazelcastMergeNode, times(1)).handleUpdateJoinKeyIfNeed(tapdataEvent);
		}

		@Test
		@DisplayName("when enable both parent and children update join key")
		void whenEnableBothParentAndChildrenUpdateJoinKey() {
			List<HazelcastProcessorBaseNode.BatchEventWrapper> batchEventWrappers = new ArrayList<>();
			TapdataEvent tapdataEvent = new TapdataEvent();
			batchEventWrappers.add(new HazelcastProcessorBaseNode.BatchEventWrapper(tapdataEvent));
			HazelcastMergeNode.EnableUpdateJoinKey enableUpdateJoinKey = new HazelcastMergeNode.EnableUpdateJoinKey();
			enableUpdateJoinKey.enableChildren();
			enableUpdateJoinKey.enableParent();
			doReturn("1").when(hazelcastMergeNode).getPreNodeId(tapdataEvent);
			Map<String, HazelcastMergeNode.EnableUpdateJoinKey> enableUpdateJoinKeyMap = new HashMap<>();
			enableUpdateJoinKeyMap.put("1", enableUpdateJoinKey);
			ReflectionTestUtils.setField(hazelcastMergeNode, "enableUpdateJoinKeyMap", enableUpdateJoinKeyMap);
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKeyIfNeed(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKeyIfNeed(batchEventWrappers);
			verify(hazelcastMergeNode, times(1)).handleUpdateJoinKeyIfNeed(tapdataEvent);
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
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKeyIfNeed(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKeyIfNeed(batchEventWrappers);
			verify(hazelcastMergeNode, times(0)).handleUpdateJoinKeyIfNeed(tapdataEvent);
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
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKeyIfNeed(tapdataEvent);
			hazelcastMergeNode.initHandleUpdateJoinKeyThreadPool();

			hazelcastMergeNode.handleBatchUpdateJoinKeyIfNeed(batchEventWrappers);
			verify(hazelcastMergeNode, times(0)).handleUpdateJoinKeyIfNeed(tapdataEvent);
		}

		@Test
		@DisplayName("when input batch event list is null or empty")
		void whenInputBatchEventsNullOrEmpty() {
			doAnswer(invocationOnMock -> null).when(hazelcastMergeNode).handleUpdateJoinKeyIfNeed(any());
			assertDoesNotThrow(() -> hazelcastMergeNode.handleBatchUpdateJoinKeyIfNeed(null));
			verify(hazelcastMergeNode, times(0)).handleUpdateJoinKeyIfNeed(any());

			assertDoesNotThrow(() -> hazelcastMergeNode.handleBatchUpdateJoinKeyIfNeed(new ArrayList<>()));
			verify(hazelcastMergeNode, times(0)).handleUpdateJoinKeyIfNeed(any());
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

			mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent);
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

			mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent);
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

			mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent);
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

			mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent);
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

			mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent);
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
			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent));

			tapUpdateRecordEvent.setAfter(new HashMap<>());
			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent));
		}

		@Test
		@SneakyThrows
		@DisplayName("when source connection's capability have before, but event not have before")
		void sourceHaveBeforeAndEventNotHaveBefore() {
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

			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent));
			assertEquals(TaskMergeProcessorExCode_16.GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_SOURCE_MUST_HAVE_BEFORE, tapCodeException.getCode());
		}

		@Test
		@DisplayName("when input tapdata event is null")
		void testTapdataEventIsNull() {
			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(null));
		}

		@Test
		@DisplayName("when input tapdata event is not dml")
		void testNotDMLEvent() {
			TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapAlterFieldNameEvent);
			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent));
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertNull(mergeInfoObj);
		}

		@Test
		@DisplayName("when is not update record event")
		void testIsNotUpdateRecordEvent() {
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			assertDoesNotThrow(() -> mockHazelcastMergeNode.handleUpdateJoinKeyIfNeed(tapdataEvent));
			Object mergeInfoObj = tapdataEvent.getTapEvent().getInfo(MergeInfo.EVENT_INFO_KEY);
			assertNull(mergeInfoObj);
		}
	}
}

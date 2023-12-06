package io.tapdata.flow.engine.V2.entity;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.AppType;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.error.PdkStateMapExCode_28;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import lombok.SneakyThrows;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PdkStateMapTest {
	private PdkStateMap mockPdkStateMap;
	private HazelcastInstance mockHazelcastInstance;

	@BeforeEach
	void beforeEach() {
		mockPdkStateMap = mock(PdkStateMap.class);
		mockHazelcastInstance = mock(HazelcastInstance.class);
	}

	@Nested
	@DisplayName("GetStateMapName Method Test")
	class GetStateMapNameTest {
		@Test
		@DisplayName("Main process test")
		void testGetStateMapName() {
			String nodeId = "1";
			String actual = PdkStateMap.getStateMapName(nodeId);
			assertEquals(PdkStateMap.class.getSimpleName() + "_" + nodeId, actual);
		}

		@Test
		@DisplayName("When node id is null")
		void testGetStateMapNameWhenNodeIdIsNull() {
			assertThrows(IllegalArgumentException.class, () -> PdkStateMap.getStateMapName(null));
		}

		@Test
		@DisplayName("When node id is blank")
		void testGetStateMapNameWhenNodeIdIsBlank() {
			assertThrows(IllegalArgumentException.class, () -> PdkStateMap.getStateMapName(""));
		}
	}

	@Nested
	@DisplayName("InitNodeStateMap Methods Test")
	class InitNodeStateMapTest {
		private ExternalStorageDto externalStorageDto;
		private DocumentIMap imapV1;
		private DocumentIMap imapV2;
		private String mapName;

		@BeforeEach
		void beforeEach() {
			externalStorageDto = new ExternalStorageDto();
			externalStorageDto = spy(externalStorageDto);
			externalStorageDto.setTtlDay(100);
			imapV1 = mock(DocumentIMap.class);
			imapV2 = mock(DocumentIMap.class);
			Logger logger = mock(Logger.class);
			when(logger.isDebugEnabled()).thenReturn(true);
			ReflectionTestUtils.setField(mockPdkStateMap, "logger", logger);
			mapName = PdkStateMap.getStateMapName("1");
			doCallRealMethod().when(mockPdkStateMap).initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto);
		}

		@Test
		@DisplayName("Main process tests")
		void testInitNodeStateMap() {
			when(imapV2.isEmpty()).thenReturn(false);
			when(mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV1);
			when(mockPdkStateMap.initDocumentIMapV2(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV2);
			mockPdkStateMap.initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto);
			Object actual = ReflectionTestUtils.getField(mockPdkStateMap, "constructIMap");
			verify(externalStorageDto, times(1)).setTable(null);
			verify(externalStorageDto, times(1)).setTtlDay(0);
			assertNotNull(actual);
			assertEquals(imapV2, actual);
			verify(mockPdkStateMap, times(1)).writeStateMapSign();
		}

		@Test
		@SneakyThrows
		@DisplayName("When imap v1 is not empty and imap v2 is empty")
		void testInitNodeStateMapV1IsNotEmptyV2IsEmpty() {
			when(imapV1.isEmpty()).thenReturn(false);
			when(imapV2.isEmpty()).thenReturn(true);
			when(mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV1);
			when(mockPdkStateMap.initDocumentIMapV2(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV2);
			mockPdkStateMap.initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto);
			Object actual = ReflectionTestUtils.getField(mockPdkStateMap, "constructIMap");
			verify(imapV2, times(1)).clear();
			verify(imapV2, times(1)).destroy();
			assertNotNull(actual);
			assertEquals(imapV1, actual);
			verify(mockPdkStateMap, times(1)).writeStateMapSign();
		}

		@Test
		@SneakyThrows
		@DisplayName("When imap v1 is empty and imap v2 is empty")
		void testInitNodeStateMapV1IsEmptyV2IsEmpty() {
			when(imapV1.isEmpty()).thenReturn(true);
			when(imapV2.isEmpty()).thenReturn(true);
			when(mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV1);
			when(mockPdkStateMap.initDocumentIMapV2(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV2);
			mockPdkStateMap.initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto);
			Object actual = ReflectionTestUtils.getField(mockPdkStateMap, "constructIMap");
			verify(imapV1, times(1)).clear();
			verify(imapV1, times(1)).destroy();
			assertNotNull(actual);
			assertEquals(imapV2, actual);
			verify(mockPdkStateMap, times(1)).writeStateMapSign();
		}

		@Test
		@DisplayName("When init imap v2 error")
		void testInitNodeStateMapInitV2Error() {
			when(imapV1.isEmpty()).thenReturn(true);
			when(imapV2.isEmpty()).thenReturn(true);
			when(mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV1);
			when(mockPdkStateMap.initDocumentIMapV2(mockHazelcastInstance, mapName, externalStorageDto)).thenThrow(RuntimeException.class);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> mockPdkStateMap.initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto));
			assertEquals(PdkStateMapExCode_28.INIT_PDK_STATE_MAP_FAILED, tapCodeException.getCode());
		}

		@Test
		@DisplayName("When init imap v1 error")
		void testInitNodeStateMapInitV1Error() {
			when(imapV1.isEmpty()).thenReturn(true);
			when(imapV2.isEmpty()).thenReturn(true);
			when(mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, mapName, externalStorageDto)).thenThrow(RuntimeException.class);
			when(mockPdkStateMap.initDocumentIMapV2(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV2);
			mockPdkStateMap.initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto);
			Object actual = ReflectionTestUtils.getField(mockPdkStateMap, "constructIMap");
			assertNotNull(actual);
			assertEquals(imapV2, actual);
			verify(mockPdkStateMap, times(1)).writeStateMapSign();
		}

		@Test
		@SneakyThrows
		@DisplayName("When imap v2 clear error")
		void testInitNodeStateMapClearV2Error() {
			when(imapV1.isEmpty()).thenReturn(false);
			when(imapV2.isEmpty()).thenReturn(true);
			when(mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV1);
			when(mockPdkStateMap.initDocumentIMapV2(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV2);
			doThrow(RuntimeException.class).when(imapV2).clear();
			assertDoesNotThrow(() -> mockPdkStateMap.initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto));
		}

		@Test
		@SneakyThrows
		@DisplayName("When imap v1 clear error")
		void testInitNodeStateMapClearV1Error() {
			when(imapV1.isEmpty()).thenReturn(true);
			when(imapV2.isEmpty()).thenReturn(true);
			when(mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV1);
			when(mockPdkStateMap.initDocumentIMapV2(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV2);
			doThrow(RuntimeException.class).when(imapV1).clear();
			assertDoesNotThrow(() -> mockPdkStateMap.initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto));
		}

		@Test
		@SneakyThrows
		@DisplayName("When init imap v1 return null")
		void testInitNodeStateMapInitIMapV1ReturnNull() {
			when(imapV2.isEmpty()).thenReturn(true);
			when(mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(null);
			when(mockPdkStateMap.initDocumentIMapV2(mockHazelcastInstance, mapName, externalStorageDto)).thenReturn(imapV2);
			mockPdkStateMap.initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto);
			Object actual = ReflectionTestUtils.getField(mockPdkStateMap, "constructIMap");
			assertNotNull(actual);
			assertEquals(imapV2, actual);
			verify(mockPdkStateMap, times(1)).writeStateMapSign();
		}
	}

	@Nested
	@DisplayName("InitConstructMap Method Test")
	class InitConstructMapTest {
		private ExternalStorageDto externalStorageDto;
		private String mapName;

		@BeforeEach
		void beforeEach() {
			externalStorageDto = new ExternalStorageDto();
			mapName = PdkStateMap.getStateMapName("1");
			doCallRealMethod().when(mockPdkStateMap).initConstructMap(eq(mockHazelcastInstance), anyString());
		}

		@Test
		@DisplayName("When app type is cloud")
		void testInitConstructMapWhenCloud() {
			try (MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class)) {
				appTypeMockedStatic.when(AppType::init).thenReturn(AppType.DFS);
				mockPdkStateMap.initConstructMap(mockHazelcastInstance, mapName);
				verify(mockPdkStateMap, times(1)).initHttpTMStateMap(mockHazelcastInstance, null, mapName);
			}
		}

		@Test
		@DisplayName("When app type is not cloud and init global state map")
		void testInitConstructMapWhenNotCloudAndInitGlobalStateMap() {
			try (
					MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class);
					MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class)
			) {
				appTypeMockedStatic.when(AppType::init).thenReturn(AppType.DAAS);
				externalStorageUtilMockedStatic.when(ExternalStorageUtil::getTapdataOrDefaultExternalStorage).thenReturn(externalStorageDto);
				mapName = Objects.requireNonNull(ReflectionTestUtils.getField(mockPdkStateMap, "GLOBAL_MAP_NAME")).toString();
				mockPdkStateMap.initConstructMap(mockHazelcastInstance, mapName);
				verify(mockPdkStateMap, times(1)).initGlobalStateMap(mockHazelcastInstance, mapName, externalStorageDto);
			}
		}

		@Test
		@DisplayName("When app type is not cloud and init node state map")
		void testInitConstructMapWhenNotCloudAndInitNodeStateMap() {
			try (
					MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class);
					MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class)
			) {
				appTypeMockedStatic.when(AppType::init).thenReturn(AppType.DAAS);
				externalStorageUtilMockedStatic.when(ExternalStorageUtil::getTapdataOrDefaultExternalStorage).thenReturn(externalStorageDto);
				mockPdkStateMap.initConstructMap(mockHazelcastInstance, mapName);
				verify(mockPdkStateMap, times(1)).initNodeStateMap(mockHazelcastInstance, mapName, externalStorageDto);
			}
		}
	}

	@Nested
	@DisplayName("InitGlobalStateMap Method Test")
	class InitGlobalStateMapTest {

		private ExternalStorageDto externalStorageDto;
		private DocumentIMap iMap;

		@BeforeEach
		void beforeEach() {
			doCallRealMethod().when(mockPdkStateMap).initGlobalStateMap(eq(mockHazelcastInstance), anyString(), any(ExternalStorageDto.class));
			externalStorageDto = new ExternalStorageDto();
			iMap = mock(DocumentIMap.class);
		}

		@Test
		@DisplayName("Main process test")
		void testInitGlobalStateMap() {
			when(mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, "", externalStorageDto))
					.thenAnswer(invocationOnMock -> {
						Object argument2 = invocationOnMock.getArgument(2);
						assertNotNull(argument2);
						assertInstanceOf(ExternalStorageDto.class, argument2);
						assertEquals(PdkStateMap.STATE_MAP_TABLE, ((ExternalStorageDto) argument2).getTable());
						return iMap;
					});
			mockPdkStateMap.initGlobalStateMap(mockHazelcastInstance, "", externalStorageDto);
			Object actual = ReflectionTestUtils.getField(mockPdkStateMap, "constructIMap");
			verify(mockPdkStateMap, times(1)).initDocumentIMapV1(mockHazelcastInstance, "", externalStorageDto);
			assertNotNull(actual);
			assertEquals(iMap, actual);
		}
	}

	@Nested
	@DisplayName("InitDocumentIMap Method Test")
	class InitDocumentIMapTest {

		private ExternalStorageDto externalStorageDto;
		private DocumentIMap documentIMap;

		@BeforeEach
		void beforeEach() {
			externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setTable("test");
			when(mockPdkStateMap.initDocumentIMapV1(eq(mockHazelcastInstance), anyString(), any(ExternalStorageDto.class))).thenCallRealMethod();
			when(mockPdkStateMap.initDocumentIMapV2(eq(mockHazelcastInstance), anyString(), any(ExternalStorageDto.class))).thenCallRealMethod();
			documentIMap = mock(DocumentIMap.class);
		}

		@Test
		@DisplayName("Init documentIMap V1 main process test")
		void testInitDocumentIMapV1() {
			when(mockPdkStateMap.initDocumentIMap(eq(mockHazelcastInstance), anyString(), any(ExternalStorageDto.class))).thenReturn(documentIMap);
			DocumentIMap<Document> iMapV1 = mockPdkStateMap.initDocumentIMapV1(mockHazelcastInstance, "test", externalStorageDto);
			assertNotNull(iMapV1);
			assertEquals(documentIMap, iMapV1);
			verify(mockPdkStateMap, times(1)).initDocumentIMap(eq(mockHazelcastInstance), anyString(), any(ExternalStorageDto.class));
		}

		@Test
		@DisplayName("Init documentIMap V2 main process test")
		void testInitDocumentIMapV2() {
			AtomicReference<String> actualMapName = new AtomicReference<>();
			when(mockPdkStateMap.initDocumentIMap(eq(mockHazelcastInstance), anyString(), any(ExternalStorageDto.class))).thenAnswer(invocationOnMock -> {
				actualMapName.set(invocationOnMock.getArgument(1));
				return documentIMap;
			});
			DocumentIMap<Document> iMapV2 = mockPdkStateMap.initDocumentIMapV2(mockHazelcastInstance, "test", externalStorageDto);
			assertNotNull(iMapV2);
			assertEquals(documentIMap, iMapV2);
			assertNull(externalStorageDto.getTable());
			assertEquals(String.valueOf("test".hashCode()), actualMapName.get());
			verify(mockPdkStateMap, times(1)).initDocumentIMap(eq(mockHazelcastInstance), anyString(), any(ExternalStorageDto.class));
		}
	}

	@Nested
	@DisplayName("WriteStateMapSign Method Test")
	class WriteStateMapSignTest {

		private DocumentIMap documentIMap;

		@BeforeEach
		void beforeEach() {
			doCallRealMethod().when(mockPdkStateMap).writeStateMapSign();
			documentIMap = mock(DocumentIMap.class);
			ReflectionTestUtils.setField(mockPdkStateMap, "constructIMap", documentIMap);
		}

		@Test
		@SneakyThrows
		@DisplayName("When node is not null and node id is null")
		void testWriteStateMapSignNodeIsNotNullAndNodeIdIsNull() {
			AtomicReference<Object> actual1 = new AtomicReference<>();
			AtomicReference<Object> actual2 = new AtomicReference<>();
			Node node = mock(Node.class);
			when(node.getId()).thenReturn("1");
			when(node.getName()).thenReturn("test");
			ReflectionTestUtils.setField(mockPdkStateMap, "node", node);
			ReflectionTestUtils.setField(mockPdkStateMap, "nodeId", "2");
			ReflectionTestUtils.setField(mockPdkStateMap, "stateMapVersion", PdkStateMap.StateMapVersion.V2);
			doAnswer(invocationOnMock -> {
				actual1.set(invocationOnMock.getArgument(0));
				actual2.set(invocationOnMock.getArgument(1));
				return null;
			}).when(documentIMap).insert(anyString(), any(Document.class));
			mockPdkStateMap.writeStateMapSign();
			assertInstanceOf(String.class, actual1.get());
			assertInstanceOf(Document.class, actual2.get());
			Document doc = (Document) actual2.get();
			assertEquals(5, doc.size());
			assertEquals("1", doc.getString("nodeId"));
			assertEquals("test", doc.getString("nodeName"));
			assertEquals(node.getClass().getName(), doc.getString("nodeClass"));
			assertEquals(PdkStateMap.getStateMapName("1"), doc.getString("stateMapName"));
			assertEquals(PdkStateMap.StateMapVersion.V2.name(), doc.getString("stateMapVersion"));
			verify(documentIMap, times(1)).insert(anyString(), any(Document.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("When node is not null and node id is not null")
		void testWriteStateMapSignNodeIsNotNullAndNodeIdIsNotNull() {
			AtomicReference<Object> actual1 = new AtomicReference<>();
			AtomicReference<Object> actual2 = new AtomicReference<>();
			Node node = mock(Node.class);
			when(node.getId()).thenReturn("1");
			when(node.getName()).thenReturn("test");
			ReflectionTestUtils.setField(mockPdkStateMap, "node", node);
			ReflectionTestUtils.setField(mockPdkStateMap, "nodeId", null);
			ReflectionTestUtils.setField(mockPdkStateMap, "stateMapVersion", PdkStateMap.StateMapVersion.V2);
			doAnswer(invocationOnMock -> {
				actual1.set(invocationOnMock.getArgument(0));
				actual2.set(invocationOnMock.getArgument(1));
				return null;
			}).when(documentIMap).insert(anyString(), any(Document.class));
			mockPdkStateMap.writeStateMapSign();
			assertInstanceOf(String.class, actual1.get());
			assertInstanceOf(Document.class, actual2.get());
			Document doc = (Document) actual2.get();
			assertEquals(5, doc.size());
			assertEquals("1", doc.getString("nodeId"));
			assertEquals("test", doc.getString("nodeName"));
			assertEquals(node.getClass().getName(), doc.getString("nodeClass"));
			assertEquals(PdkStateMap.getStateMapName("1"), doc.getString("stateMapName"));
			assertEquals(PdkStateMap.StateMapVersion.V2.name(), doc.getString("stateMapVersion"));
			verify(documentIMap, times(1)).insert(anyString(), any(Document.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("When node is null and node id is not null")
		void testWriteStateMapSignNodeIdIsNotNull() {
			AtomicReference<Object> actual1 = new AtomicReference<>();
			AtomicReference<Object> actual2 = new AtomicReference<>();
			ReflectionTestUtils.setField(mockPdkStateMap, "nodeId", "1");
			ReflectionTestUtils.setField(mockPdkStateMap, "stateMapVersion", PdkStateMap.StateMapVersion.V2);
			doAnswer(invocationOnMock -> {
				actual1.set(invocationOnMock.getArgument(0));
				actual2.set(invocationOnMock.getArgument(1));
				return null;
			}).when(documentIMap).insert(anyString(), any(Document.class));
			mockPdkStateMap.writeStateMapSign();
			assertInstanceOf(String.class, actual1.get());
			assertInstanceOf(Document.class, actual2.get());
			Document doc = (Document) actual2.get();
			assertEquals(3, doc.size());
			assertEquals("1", doc.getString("nodeId"));
			assertEquals(PdkStateMap.getStateMapName("1"), doc.getString("stateMapName"));
			assertEquals(PdkStateMap.StateMapVersion.V2.name(), doc.getString("stateMapVersion"));
			verify(documentIMap, times(1)).insert(anyString(), any(Document.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("When node and node id both are null")
		void testWriteStateMapSignNodeAndNodeIdBothNull() {
			ReflectionTestUtils.setField(mockPdkStateMap, "node", null);
			ReflectionTestUtils.setField(mockPdkStateMap, "nodeId", null);
			mockPdkStateMap.writeStateMapSign();
			verify(documentIMap, times(0)).insert(anyString(), any(Document.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("When constructIMap is null")
		void testWriteStateMapSignWhenConstructIMapIsNull() {
			ReflectionTestUtils.setField(mockPdkStateMap, "constructIMap", null);
			mockPdkStateMap.writeStateMapSign();
			verify(documentIMap, times(0)).insert(anyString(), any(Document.class));
		}
	}

	@Nested
	@DisplayName("InitHttpTMStateMap Method Test")
	class InitHttpTMStateMapTest {

		private ConfigurationCenter configurationCenter;
		private String mapName;
		private String url;
		private String accessCode;
		private DocumentIMap documentIMap;

		@BeforeEach
		void beforeEach() {
			url = "http://localhost:3030";
			List<String> urls = Collections.singletonList(url);
			accessCode = "xxx";
			configurationCenter = mock(ConfigurationCenter.class);
			when(configurationCenter.getConfig(ConfigurationCenter.BASR_URLS)).thenReturn(urls);
			when(configurationCenter.getConfig(ConfigurationCenter.ACCESS_CODE)).thenReturn(accessCode);
			doCallRealMethod().when(mockPdkStateMap).initHttpTMStateMap(eq(mockHazelcastInstance), any(ConfigurationCenter.class), anyString());
			mapName = PdkStateMap.getStateMapName("1");
			documentIMap = mock(DocumentIMap.class);
		}

		@Test
		@DisplayName("Main process test")
		void testInitHttpTMStateMap() {
			AtomicReference<Object> actual = new AtomicReference<>();
			when(mockPdkStateMap.initDocumentIMap(eq(mockHazelcastInstance), anyString(), any(ExternalStorageDto.class))).thenAnswer(invocationOnMock -> {
				actual.set(invocationOnMock.getArgument(2));
				return documentIMap;
			});
			mockPdkStateMap.initHttpTMStateMap(mockHazelcastInstance, configurationCenter, mapName);
			verify(mockPdkStateMap, times(1)).initDocumentIMap(eq(mockHazelcastInstance), anyString(), any(ExternalStorageDto.class));
			assertNotNull(actual.get());
			assertInstanceOf(ExternalStorageDto.class, actual.get());
			ExternalStorageDto actualEx = (ExternalStorageDto) actual.get();
			assertEquals(ExternalStorageType.httptm.name(), actualEx.getType());
			assertNotNull(actualEx.getBaseURLs());
			assertEquals(1, actualEx.getBaseURLs().size());
			assertEquals(url, actualEx.getBaseURLs().get(0));
			assertNotNull(actualEx.getAccessToken());
			assertEquals(accessCode, actualEx.getAccessToken());
			assertEquals(PdkStateMap.CONNECT_TIMEOUT_MS, actualEx.getConnectTimeoutMs());
			assertEquals(PdkStateMap.READ_TIMEOUT_MS, actualEx.getReadTimeoutMs());
		}

		@Test
		@DisplayName("When configurationCenter is null")
		void testInitHttpTMStateMapWhenConfigurationCenterIsNull() {
			doCallRealMethod().when(mockPdkStateMap).initHttpTMStateMap(eq(mockHazelcastInstance), isNull(), anyString());
			assertThrows(IllegalArgumentException.class, () -> mockPdkStateMap.initHttpTMStateMap(mockHazelcastInstance, null, mapName));
		}

		@Test
		@DisplayName("When base urls is null")
		void testInitHttpTMStateMapBaseUrlsIsNull() {
			when(configurationCenter.getConfig(ConfigurationCenter.BASR_URLS)).thenReturn(null);
			assertThrows(IllegalArgumentException.class, () -> mockPdkStateMap.initHttpTMStateMap(mockHazelcastInstance, configurationCenter, mapName));
		}

		@Test
		@DisplayName("When base urls is not list")
		void testInitHttpTMStateMapBaseUrlsIsNotList() {
			when(configurationCenter.getConfig(ConfigurationCenter.BASR_URLS)).thenReturn(url);
			assertThrows(IllegalArgumentException.class, () -> mockPdkStateMap.initHttpTMStateMap(mockHazelcastInstance, configurationCenter, mapName));
		}

		@Test
		@DisplayName("When base urls is empty")
		void testInitHttpTMStateMapBaseUrlsIsEmpty() {
			when(configurationCenter.getConfig(ConfigurationCenter.BASR_URLS)).thenReturn(new ArrayList<>());
			assertThrows(IllegalArgumentException.class, () -> mockPdkStateMap.initHttpTMStateMap(mockHazelcastInstance, configurationCenter, mapName));
		}

		@Test
		@DisplayName("When access code is null")
		void testInitHttpTMStateMapAccessCodeIsNull() {
			when(configurationCenter.getConfig(ConfigurationCenter.ACCESS_CODE)).thenReturn(null);
			assertThrows(IllegalArgumentException.class, () -> mockPdkStateMap.initHttpTMStateMap(mockHazelcastInstance, configurationCenter, mapName));
		}

		@Test
		@DisplayName("When access code is not string")
		void testInitHttpTMStateMapAccessCodeIsNotString() {
			when(configurationCenter.getConfig(ConfigurationCenter.ACCESS_CODE)).thenReturn(1);
			assertThrows(IllegalArgumentException.class, () -> mockPdkStateMap.initHttpTMStateMap(mockHazelcastInstance, configurationCenter, mapName));
		}
	}

	@Nested
	@DisplayName("Operation(put, get, remove...) Method Test")
	class OperationTest {

		private DocumentIMap documentIMap;
		private final String key = "key";
		private final Object value = "value";

		@BeforeEach
		void beforeEach() {
			documentIMap = mock(DocumentIMap.class);
			ReflectionTestUtils.setField(mockPdkStateMap, "constructIMap", documentIMap);
		}

		@Test
		@SneakyThrows
		@DisplayName("Put method test")
		void testPut() {
			when(documentIMap.insert(anyString(), any())).thenAnswer(invocationOnMock -> {
				Object argument = invocationOnMock.getArgument(0);
				assertNotNull(argument);
				assertInstanceOf(String.class, argument);
				assertEquals(key, argument);
				argument = invocationOnMock.getArgument(1);
				assertNotNull(argument);
				assertInstanceOf(Document.class, argument);
				assertTrue(((Document) argument).containsKey(PdkStateMap.class.getSimpleName()));
				assertEquals(value, ((Document) argument).get(PdkStateMap.class.getSimpleName()));
				return null;
			});
			doCallRealMethod().when(mockPdkStateMap).put(anyString(), any());
			mockPdkStateMap.put(key, value);
			verify(documentIMap, times(1)).insert(anyString(), any());
		}

		@Test
		@SneakyThrows
		@DisplayName("PutIfAbsent method test")
		void testPutIfAbsent() {
			IMap iMap = mock(IMap.class);
			when(iMap.putIfAbsent(anyString(), any())).thenAnswer(invocationOnMock -> {
				Object argument = invocationOnMock.getArgument(0);
				assertNotNull(argument);
				assertInstanceOf(String.class, argument);
				assertEquals(key, argument);
				argument = invocationOnMock.getArgument(1);
				assertNotNull(argument);
				assertInstanceOf(Document.class, argument);
				assertTrue(((Document) argument).containsKey(PdkStateMap.class.getSimpleName()));
				assertEquals(value, ((Document) argument).get(PdkStateMap.class.getSimpleName()));
				return null;
			});
			when(documentIMap.getiMap()).thenReturn(iMap);
			when(mockPdkStateMap.putIfAbsent(anyString(), any())).thenCallRealMethod();
			mockPdkStateMap.putIfAbsent(key, value);
			verify(iMap, times(1)).putIfAbsent(anyString(), any());
		}

		@Test
		@SneakyThrows
		@DisplayName("Remove method test")
		void testRemove() {
			when(documentIMap.delete(anyString())).thenAnswer(invocationOnMock -> {
				Object argument = invocationOnMock.getArgument(0);
				assertNotNull(argument);
				assertInstanceOf(String.class, argument);
				assertEquals(key, argument);
				return 1;
			});
			when(mockPdkStateMap.remove(anyString())).thenCallRealMethod();
			Object actual = mockPdkStateMap.remove(key);
			assertEquals(1, actual);
			verify(documentIMap, times(1)).delete(anyString());
		}

		@Test
		@SneakyThrows
		@DisplayName("Clear method test")
		void testClear() {
			doCallRealMethod().when(mockPdkStateMap).clear();
			mockPdkStateMap.clear();
			verify(documentIMap, times(1)).clear();
		}

		@Test
		@SneakyThrows
		@DisplayName("Reset method test")
		void testReset() {
			doCallRealMethod().when(mockPdkStateMap).reset();
			mockPdkStateMap.reset();
			verify(documentIMap, times(1)).destroy();
		}

		@Test
		@SneakyThrows
		@DisplayName("Get method main process test")
		void testGet() {
			when(documentIMap.find(anyString())).thenReturn(new Document(PdkStateMap.class.getSimpleName(), value));
			when(mockPdkStateMap.get(anyString())).thenCallRealMethod();
			Object actual = mockPdkStateMap.get(key);
			verify(documentIMap, times(1)).find(anyString());
			assertNotNull(actual);
			assertInstanceOf(value.getClass(), actual);
			assertEquals(value, actual);
		}

		@Test
		@SneakyThrows
		@DisplayName("Get method when find return null test")
		void testGetFindReturnNull() {
			when(documentIMap.find(anyString())).thenReturn(null);
			when(mockPdkStateMap.get(anyString())).thenCallRealMethod();
			Object actual = mockPdkStateMap.get(key);
			verify(documentIMap, times(1)).find(anyString());
			assertNull(actual);
		}
	}

	@Nested
	@DisplayName("Construct Method Test")
	class ConstructTest {

		private ExternalStorageDto externalStorageDto;
		private PersistenceStorage persistenceStorage;
		private IMap iMap;
		private Node node;

		@BeforeEach
		void beforeEach() {
			externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setId(new ObjectId());
			externalStorageDto.setType(ExternalStorageType.mongodb.getMode());
			externalStorageDto.setUri("mongodb://localhost:27017/test");
			externalStorageDto.setTtlDay(3);
			persistenceStorage = spy(PersistenceStorage.getInstance());
			iMap = mock(IMap.class);
			when(iMap.isEmpty()).thenReturn(false);
			doReturn(persistenceStorage).when(persistenceStorage).initMapStoreConfig(anyString(), any(Config.class), anyString());
			doReturn(true).when(persistenceStorage).isEmpty(eq(ConstructType.IMAP), anyString());
			doReturn(iMap).when(mockHazelcastInstance).getMap(anyString());
			node = mock(Node.class);
			when(node.getId()).thenReturn("1");
		}

		@Test
		@DisplayName("Main process test(String nodeId, HazelcastInstance hazelcastInstance)")
		void construct1() {
			try (
					MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class);
					MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class);
					MockedStatic<PersistenceStorage> persistenceStorageMockedStatic = mockStatic(PersistenceStorage.class)
			) {
				persistenceStorageMockedStatic.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
				appTypeMockedStatic.when(AppType::init).thenReturn(AppType.DAAS);
				externalStorageUtilMockedStatic.when(ExternalStorageUtil::getTapdataOrDefaultExternalStorage).thenReturn(externalStorageDto);
				externalStorageUtilMockedStatic.when(() -> ExternalStorageUtil.initHZMapStorage(eq(externalStorageDto), anyString(), anyString(), any(Config.class))).thenAnswer(invocationOnMock -> null);
				PdkStateMap pdkStateMap = new PdkStateMap(node.getId(), mockHazelcastInstance);
				assertNotNull(pdkStateMap);
				Object actual = ReflectionTestUtils.getField(pdkStateMap, "constructIMap");
				assertNotNull(actual);
				assertInstanceOf(DocumentIMap.class, actual);
				assertNotNull(((DocumentIMap) actual).getiMap());
				assertEquals(iMap, ((DocumentIMap) actual).getiMap());
				assertEquals(0, externalStorageDto.getTtlDay());
			}
		}

		@Test
		@DisplayName("Main process test(HazelcastInstance hazelcastInstance, Node<?> node)")
		void construct2() {
			try (
					MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class);
					MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class);
					MockedStatic<PersistenceStorage> persistenceStorageMockedStatic = mockStatic(PersistenceStorage.class)
			) {
				persistenceStorageMockedStatic.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
				appTypeMockedStatic.when(AppType::init).thenReturn(AppType.DAAS);
				externalStorageUtilMockedStatic.when(ExternalStorageUtil::getTapdataOrDefaultExternalStorage).thenReturn(externalStorageDto);
				externalStorageUtilMockedStatic.when(() -> ExternalStorageUtil.initHZMapStorage(eq(externalStorageDto), anyString(), anyString(), any(Config.class))).thenAnswer(invocationOnMock -> null);
				PdkStateMap pdkStateMap = new PdkStateMap(mockHazelcastInstance, node);
				assertNotNull(pdkStateMap);
				Object actualConstructIMap = ReflectionTestUtils.getField(pdkStateMap, "constructIMap");
				assertNotNull(actualConstructIMap);
				assertInstanceOf(DocumentIMap.class, actualConstructIMap);
				assertNotNull(((DocumentIMap) actualConstructIMap).getiMap());
				assertEquals(iMap, ((DocumentIMap) actualConstructIMap).getiMap());
				assertEquals(0, externalStorageDto.getTtlDay());
			}
		}
	}

	@Nested
	@DisplayName("GlobalStateMap Method Test")
	class GlobalStateMapTest {
		@Test
		void testGlobalStateMap() {
			ExternalStorageDto externalStorageDto = new ExternalStorageDto();
			externalStorageDto.setId(new ObjectId());
			externalStorageDto.setType(ExternalStorageType.mongodb.getMode());
			externalStorageDto.setUri("mongodb://localhost:27017/test");
			externalStorageDto.setTtlDay(3);
			PersistenceStorage persistenceStorage = spy(PersistenceStorage.getInstance());
			IMap iMap = mock(IMap.class);
			when(iMap.isEmpty()).thenReturn(false);
			doReturn(persistenceStorage).when(persistenceStorage).initMapStoreConfig(anyString(), any(Config.class), anyString());
			doReturn(iMap).when(mockHazelcastInstance).getMap(anyString());
			try (
					MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class);
					MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class);
					MockedStatic<PersistenceStorage> persistenceStorageMockedStatic = mockStatic(PersistenceStorage.class)
			) {
				persistenceStorageMockedStatic.when(PersistenceStorage::getInstance).thenReturn(persistenceStorage);
				appTypeMockedStatic.when(AppType::init).thenReturn(AppType.DAAS);
				externalStorageUtilMockedStatic.when(ExternalStorageUtil::getTapdataOrDefaultExternalStorage).thenReturn(externalStorageDto);
				externalStorageUtilMockedStatic.when(() -> ExternalStorageUtil.initHZMapStorage(eq(externalStorageDto), anyString(), anyString(), any(Config.class))).thenAnswer(invocationOnMock -> null);
				PdkStateMap pdkStateMap = PdkStateMap.globalStateMap(mockHazelcastInstance);
				assertNotNull(pdkStateMap);
				Object actualConstructIMap = ReflectionTestUtils.getField(pdkStateMap, "constructIMap");
				assertNotNull(actualConstructIMap);
				assertInstanceOf(DocumentIMap.class, actualConstructIMap);
				assertNotNull(((DocumentIMap) actualConstructIMap).getiMap());
				assertEquals(iMap, ((DocumentIMap) actualConstructIMap).getiMap());
				assertEquals(0, externalStorageDto.getTtlDay());
			}
		}
	}
}

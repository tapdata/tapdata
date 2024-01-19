package io.tapdata.websocket.handler;

import base.BaseTest;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.ConnectionFunctions;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.schema.SchemaProxy;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventResult;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-27 18:07
 **/
@DisplayName("LoadSchemaEventHandler class test")
class LoadSchemaEventHandlerTest extends BaseTest {
	private LoadSchemaEventHandler loadSchemaEventHandler;

	@BeforeEach
	void beforeEach() {
		loadSchemaEventHandler = new LoadSchemaEventHandler();
		loadSchemaEventHandler.clientMongoOperator = mockClientMongoOperator;
		loadSchemaEventHandler.settingService = mockSettingService;
	}

	@Nested
	@DisplayName("Handle method test")
	class HandleTest {
		private SendMessage mockSendMessage;
		private String connId;
		private String tableName;
		private Connections connections;

		@BeforeEach
		void beforeEach() {
			loadSchemaEventHandler = spy(loadSchemaEventHandler);
			mockSendMessage = mock(SendMessage.class);
			SchemaProxy.schemaProxy = SchemaProxy.SchemaProxyInstance.INSTANCE.getInstance(mockClientMongoOperator);

			connId = "1";
			tableName = "test1";
			connections = new Connections();
			connections.setId(connId);
			connections.setPdkType("pdk");
			connections.setPdkHash("1");
		}

		@Test
		@SneakyThrows
		@DisplayName("Main process test")
		void testHandle() {
			// mock data
			List<Connections> connectionList = new ArrayList<>();
			connectionList.add(connections);
			Map<String, Object> loadSchemaEvent = new HashMap<>();
			loadSchemaEvent.put("connId", connId);
			loadSchemaEvent.put("tableName", tableName);
			List tables = new ArrayList();
			tables.add(loadSchemaEvent);
			Map<String, Object> event = new HashMap<>();
			event.put("tables", tables);
			ConnectionNode connectionNode = mock(ConnectionNode.class);
			doNothing().when(connectionNode).connectorInit();
			doNothing().when(connectionNode).connectorStop();
			Query query = new Query(Criteria.where("id").is(connId));
			query.fields().exclude("schema");
			when(mockClientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class)).thenReturn(connectionList);
			DatabaseTypeEnum.DatabaseType databaseType = new DatabaseTypeEnum.DatabaseType();
			databaseType.setGroup("test");
			databaseType.setPdkId("test");
			databaseType.setVersion("1");
			when(mockClientMongoOperator.findOne(any(Map.class), eq(ConnectorConstant.DATABASE_TYPE_COLLECTION + "/pdkHash/" + connections.getPdkHash()), eq(DatabaseTypeEnum.DatabaseType.class)))
					.thenReturn(databaseType);
			doReturn(connectionNode).when(loadSchemaEventHandler).getConnectionNode(connections, databaseType);
			TapTable testTable = new TapTable("test_table");
			List<TapTable> tapTables = new ArrayList<>();
			tapTables.add(testTable);
			doReturn(tapTables).when(loadSchemaEventHandler).loadPdkSchema(connections, connectionNode);
			List<WebSocketEventResult> messages = new ArrayList<>();
			doAnswer(invocationOnMock -> messages.add(invocationOnMock.getArgument(0))).when(mockSendMessage).send(any(WebSocketEventResult.class));

			// execute real method
			Object actual = loadSchemaEventHandler.handle(event, mockSendMessage);

			// assertions
			assertNotNull(actual);
			assertEquals(Thread.class, actual.getClass());
			assertDoesNotThrow(() -> ((Thread) actual).join());
			assertEquals(1, messages.size());
			assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_SUCCESS, messages.get(0).getStatus());
			assertEquals(ArrayList.class, messages.get(0).getResult().getClass());
			List actualResult = (List) messages.get(0).getResult();
			assertEquals(1, actualResult.size());
			assertEquals(LoadSchemaEventHandler.LoadSchemaEvent.class, actualResult.get(0).getClass());
			LoadSchemaEventHandler.LoadSchemaEvent actualLoadSchemaEvent = (LoadSchemaEventHandler.LoadSchemaEvent) actualResult.get(0);
			assertNotNull(actualLoadSchemaEvent.getSchema());
			assertEquals(testTable, actualLoadSchemaEvent.getSchema());
		}
	}

	@Nested
	@DisplayName("LoadPdkSchema method test")
	class LoadPdkSchemaTest {
		private Connections connections;
		private ConnectionNode connectionNode;
		private TapTable testTable;
		private ConnectionFunctions connectionFunctions;

		@BeforeEach
		void beforeEach() {
			connections = new Connections();
			connectionNode = mock(ConnectionNode.class);
			testTable = new TapTable("test_table");
			connectionFunctions = mock(ConnectionFunctions.class);
			when(connectionFunctions.getGetTableNamesFunction()).thenReturn(null);
			when(connectionNode.getConnectionFunctions()).thenReturn(connectionFunctions);
		}

		@Test
		@SneakyThrows
		@DisplayName("Main process test")
		void testLoadPdkSchema() {
			try (
					MockedStatic<LoadSchemaRunner> loadSchemaRunnerMockedStatic = mockStatic(LoadSchemaRunner.class);
					MockedStatic<LoadSchemaRunner.TableFilter> tableFilterMockedStatic = mockStatic(LoadSchemaRunner.TableFilter.class)
			) {
				tableFilterMockedStatic.when(() -> LoadSchemaRunner.TableFilter.create(anyString(), anyString())).thenReturn(mock(LoadSchemaRunner.TableFilter.class));
				loadSchemaRunnerMockedStatic.when(() -> LoadSchemaRunner.loadPdkSchema(eq(connections), eq(connectionNode), any(Consumer.class))).thenCallRealMethod();
				loadSchemaRunnerMockedStatic.when(() -> LoadSchemaRunner.pdkDiscoverSchema(eq(connectionNode), any(List.class), any(Consumer.class)))
						.thenAnswer(invocationOnMock -> {
							Object consumer = invocationOnMock.getArgument(2);
							if (consumer instanceof Consumer) {
								((Consumer) consumer).accept(testTable);
							}
							return null;
						});
				List<TapTable> tapTables = loadSchemaEventHandler.loadPdkSchema(connections, connectionNode);
				assertEquals(1, tapTables.size());
				assertEquals(testTable, tapTables.get(0));
			}
		}

		@Test
		@SneakyThrows
		@DisplayName("When table name blank")
		void testLoadPdkSchemaWhenTableNameIsBlank() {
			try (
					MockedStatic<LoadSchemaRunner> loadSchemaRunnerMockedStatic = mockStatic(LoadSchemaRunner.class);
					MockedStatic<LoadSchemaRunner.TableFilter> tableFilterMockedStatic = mockStatic(LoadSchemaRunner.TableFilter.class)
			) {
				tableFilterMockedStatic.when(() -> LoadSchemaRunner.TableFilter.create(anyString(), anyString())).thenReturn(mock(LoadSchemaRunner.TableFilter.class));
				loadSchemaRunnerMockedStatic.when(() -> LoadSchemaRunner.loadPdkSchema(eq(connections), eq(connectionNode), any(Consumer.class))).thenCallRealMethod();
				loadSchemaRunnerMockedStatic.when(() -> LoadSchemaRunner.pdkDiscoverSchema(eq(connectionNode), any(List.class), any(Consumer.class)))
						.thenAnswer(invocationOnMock -> {
							Object consumer = invocationOnMock.getArgument(2);
							if (consumer instanceof Consumer) {
								testTable.setName("");
								((Consumer) consumer).accept(testTable);
								testTable.setName(null);
								((Consumer) consumer).accept(testTable);
							}
							return null;
						});
				List<TapTable> tapTables = loadSchemaEventHandler.loadPdkSchema(connections, connectionNode);
				assertEquals(0, tapTables.size());
			}
		}
	}

	@Nested
	@DisplayName("GetConnectionNode method test")
	class getConnectionNodeTest {
		private Connections connections;
		private DatabaseTypeEnum.DatabaseType databaseType;

		@BeforeEach
		void beforeEach() {
			connections = new Connections();
			connections.setName("name");
			connections.setUser_id("user id");
			databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
			when(databaseType.getGroup()).thenReturn("group");
			when(databaseType.getPdkId()).thenReturn("pdk id");
			when(databaseType.getVersion()).thenReturn("version");
		}

		@Test
		@DisplayName("Main process test")
		void testGetConnectionNode() {
			PDKIntegration.ConnectionConnectorBuilder connectionConnectorBuilder = new PDKIntegration.ConnectionConnectorBuilder();
			connectionConnectorBuilder = spy(connectionConnectorBuilder);
			ConnectionNode connectionNode = mock(ConnectionNode.class);
			doReturn(connectionNode).when(connectionConnectorBuilder).build();
			try (MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = mockStatic(PDKIntegration.class)) {
				pdkIntegrationMockedStatic.when(PDKIntegration::createConnectionConnectorBuilder).thenReturn(connectionConnectorBuilder);
				ConnectionNode actual = loadSchemaEventHandler.getConnectionNode(connections, databaseType);
				assertNotNull(actual);
				assertEquals(connectionNode, actual);
			}
		}
	}
}

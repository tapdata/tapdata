package io.tapdata.websocket.handler;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.ResponseBody;
import com.tapdata.entity.Schema;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.validator.ConnectionValidateResult;
import com.tapdata.validator.ConnectionValidateResultDetail;
import io.tapdata.exception.ConnectionException;
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.utils.ConnectionUpdateOperation;
import io.tapdata.utils.UnitTestUtils;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventResult;
import io.tapdata.websocket.testconnection.RocksDBTestConnectionImpl;
import io.tapdata.websocket.testconnection.TestConnection;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/14 10:16 Create
 */
class TestConnectionHandlerTest {

	private TestConnectionHandler instance;

	@BeforeEach
	void setUp() {
		instance = new TestConnectionHandler();
	}

	@Test
	void testHandleValidEvent() throws Exception {
		SendMessage mockSendMessage = mock(SendMessage.class);
		TestConnectionHandler spyHandler = spy(instance);
		Map<String, Object> eventData = new HashMap<>();

		// test empty event
		Object handle = spyHandler.handle(eventData, mockSendMessage);
		if (handle instanceof WebSocketEventResult) {
			WebSocketEventResult eventResult = (WebSocketEventResult) handle;
			assertEquals(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR, eventResult.getStatus());
			assertEquals(WebSocketEventResult.Type.TEST_CONNECTION_RESULT.getType(), eventResult.getType());
		} else {
			fail("Expected WebSocketEventResult, but got.");
		}

		// test execute handle
		String connName = "sampleName";
		String pdkHash = "samplePdkHash";
		eventData.put("type", "sampleType");
		eventData.put("pdkType", "samplePdkType");
		eventData.put("connectionId", "sampleConnectionId");
		eventData.put("name", connName);
		eventData.put("pdkHash", pdkHash);
		eventData.put("schemaVersion", "sampleSchemaVersion");
		eventData.put("databaseType", "sampleDatabaseType");
		CountDownLatch countDownLatch = new CountDownLatch(1);
		doAnswer(invocationOnMock -> {
			countDownLatch.countDown();
			return null;
		}).when(spyHandler).handleSync(eq(eventData), eq(mockSendMessage), any());
		Object result = spyHandler.handle(eventData, mockSendMessage);
		assertNull(result);
		assertTrue(countDownLatch.await(5, java.util.concurrent.TimeUnit.SECONDS));
	}

	@Test
	void testIsUpdateSchema() {
		// Create test data
		DatabaseTypeEnum databaseTypeEnumFile = DatabaseTypeEnum.FILE;
		DatabaseTypeEnum databaseTypeEnumOther = DatabaseTypeEnum.MYSQL; // or any other type
		Map<String, Object> event = new HashMap<>();
		boolean def = true; // or any other default value

		// Test case for FILE type
		assertFalse(instance.isUpdateSchema(databaseTypeEnumFile, event, def));

		// Test case for other types
		assertTrue(instance.isUpdateSchema(databaseTypeEnumOther, event, def));

		// Test case for set false
		ConnectionUpdateOperation.UPDATE_SCHEMA.set(event, false);
		assertFalse(instance.isUpdateSchema(databaseTypeEnumOther, event, false));
	}

	@Test
	void testIverLoadSchema() {
		// Create test data
		DatabaseTypeEnum databaseTypeEnumFile = DatabaseTypeEnum.FILE;
		DatabaseTypeEnum databaseTypeEnumOther = DatabaseTypeEnum.MYSQL; // or any other type
		Map<String, Object> event = new HashMap<>();
		boolean def = false; // or any other default value

		// test: for FILE type
		assertTrue(instance.everLoadSchema(databaseTypeEnumFile, event, def));

		// test: for other types
		assertFalse(instance.everLoadSchema(databaseTypeEnumOther, event, def));

		// test: for set true
		ConnectionUpdateOperation.EVER_LOAD_SCHEMA.set(event, true);
		assertTrue(instance.everLoadSchema(databaseTypeEnumOther, event, true));
	}

	@Test
	void testSetQueryAndExtParam() {
		TestConnectionHandler spyHandler = spy(instance);
		ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
		spyHandler.initialize(clientMongoOperator);

		Map<String, Object> event = new HashMap<>();
		Connections connection;
		boolean editTest = false;
		boolean isExternalStorage = true;

		// test: for not exists name
		connection = spyHandler.parseConnection(event);
		Connections result = spyHandler.setQueryAndExtParam(event, connection, editTest, isExternalStorage);
		assertNotNull(result);
		assertEquals("", result.getName());
		assertTrue(spyHandler.collection.endsWith("/set"));
		assertEquals(connection, result);

		// test: for id not blank
		ConnectionUpdateOperation.ID.set(event, "test-id");
		connection = spyHandler.parseConnection(event);
		result = spyHandler.setQueryAndExtParam(event, connection, editTest, isExternalStorage);
		assertNotNull(result);
		assertEquals(connection, result);
		assertNotNull(spyHandler.connectionIdQuery);

		// test: for editTest
		connection = spyHandler.parseConnection(event);
		result = spyHandler.setQueryAndExtParam(event, connection, true, isExternalStorage);
		assertNotNull(result);
		assertEquals(connection, result);
		assertNotNull(spyHandler.connectionIdQuery);

		isExternalStorage = false;
		// test: query by id null
		result = spyHandler.setQueryAndExtParam(event, connection, editTest, isExternalStorage);
		assertNotNull(result);
		assertEquals(connection, result);
		assertNotNull(spyHandler.connectionIdQuery);

		// test: query by id exists
		Connections newConn = mock(Connections.class);
		when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(Connections.class))).thenReturn(newConn);

		Map<String, Object> extParam = new HashMap<>();
		ConnectionUpdateOperation.EXT_PARAM.set(event, extParam);
		connection = spyHandler.parseConnection(event);
		result = spyHandler.setQueryAndExtParam(event, connection, editTest, isExternalStorage);
		assertNotNull(result);
		assertEquals(extParam, result.getExtParam());
		assertNotEquals(connection, result);
		assertNotNull(spyHandler.connectionIdQuery);
	}

	@Test
	void testResetPasswordOfPlainPassword() {
		String plainPassword = "test-plain-password";
		String databasePassword = "test-database-password";

		Connections connection = new Connections();
		Map<String, Object> event = new HashMap<>();

		// test: database and plain password is all blank
		instance.resetPasswordOfPlainPassword(connection, event);
		assertNull(connection.getDatabase_password());

		// test: database password is blank
		event.put(ConnectionUpdateOperation.PLAIN_PASSWORD.getFullKey(), plainPassword);
		instance.resetPasswordOfPlainPassword(connection, event);
		assertEquals(plainPassword, connection.getDatabase_password());

		// test: database password not blank
		connection.setDatabase_password(databasePassword);
		instance.resetPasswordOfPlainPassword(connection, event);
		assertEquals(databasePassword, connection.getDatabase_password());
	}

	@Test
	void testGetDatabaseType() {
		String pdkType = "test-pdk-type";
		String pdkHash = "test-pdk-hash";

		TestConnectionHandler spyHandler = spy(instance);
		ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
		spyHandler.initialize(clientMongoOperator, null);

		Connections connection = new Connections();

		// test: not found pdk type
		String message = assertThrows(ConnectionException.class, () -> spyHandler.getDatabaseType(connection)).getMessage();
		assertEquals("Unknown connection pdk type", message);

		connection.setPdkType(pdkType);
		message = assertThrows(ConnectionException.class, () -> spyHandler.getDatabaseType(connection)).getMessage();
		assertTrue(message.startsWith("Unknown database type"));

		try (
			MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class);
		) {
			connection.setPdkHash(pdkHash);
			connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(clientMongoOperator, pdkHash)).thenReturn(mock(DatabaseTypeEnum.DatabaseType.class));
			doNothing().when(spyHandler).downloadPdkFile(any());

			spyHandler.getDatabaseType(connection);
			verify(spyHandler, times(1)).downloadPdkFile(any());
		}
	}

	@Nested
	class ParseConnectionTest {
		@Test
		void testSuccess() {

			// test: Map convert to Connections failed
			assertThrows(ConnectionException.class, () -> instance.parseConnection(null));

			// test: remove schema
			Map<String, Object> event = new HashMap<>();
			Map<Object, Object> schema = new HashMap<>();
			ConnectionUpdateOperation.SCHEMA.set(event, schema);
			Connections result = instance.parseConnection(event);
			assertNotNull(result);
			assertFalse(event.containsKey(ConnectionUpdateOperation.SCHEMA.getFullKey()));
		}
	}

	@Nested
	class HandleSyncTest {

		Logger mockLogger;
		SendMessage mockSendMessage;
		TestConnectionHandler spyHandler;
		HttpClientMongoOperator clientMongoOperator;
		AtomicReference<Exception> errorEx = new AtomicReference();

		String connName = "sampleName";
		String pdkHash = "samplePdkHash";
		boolean isExternalStorage;
		Map<String, Object> eventData;

		@BeforeEach
		void setUp() {
			mockLogger = mock(Logger.class);
			mockSendMessage = mock(SendMessage.class);
			spyHandler = spy(instance);
			clientMongoOperator = mock(HttpClientMongoOperator.class);
			UnitTestUtils.injectField(TestConnectionHandler.class, spyHandler, "clientMongoOperator", clientMongoOperator);

			// before handle set errorEx=null
			doAnswer(invocationOnMock -> {
				errorEx.set(null);
				assertNotNull(invocationOnMock.getArgument(0));
				assertNotNull(invocationOnMock.getArgument(2));
				return invocationOnMock.callRealMethod();
			}).when(spyHandler).handleSync(any(), any(), any());

			// do handle error set errorEx=exception
			doAnswer(invocationOnMock -> {
				Exception e = invocationOnMock.getArgument(4, Exception.class);
				errorEx.set(e);
				return null;
			}).when(spyHandler).handleError(any(), any(), any(), any(), any());

			eventData = new HashMap<>();
			isExternalStorage = false;
		}

		@Test
		void testUnknownConnectionPdkType() throws Exception {
			ConnectionUpdateOperation.TYPE.set(eventData, "sampleType");
			ConnectionUpdateOperation.CONNECTION_ID.set(eventData, "sampleConnectionId");
			ConnectionUpdateOperation.NAME.set(eventData, connName);
			ConnectionUpdateOperation.PDK_HASH.set(eventData, pdkHash);
			ConnectionUpdateOperation.SCHEMA_VERSION.set(eventData, "sampleSchemaVersion");
			ConnectionUpdateOperation.DATABASE_TYPE.set(eventData, DatabaseTypeEnum.MONGODB.getType());

			// test: Unknown connection pdk type
			Connections connection = spyHandler.parseConnection(eventData);
			spyHandler.handleSync(eventData, mockSendMessage, connection);
			assertNotNull(errorEx.get());
			if (null == errorEx.get().getMessage() || !errorEx.get().getMessage().startsWith("Unknown connection pdk type")) {
				throw errorEx.get();
			}
		}

		@Test
		void testUnknowDatabaseType() throws Exception {
			ConnectionUpdateOperation.PDK_TYPE.set(eventData, "samplePdkType");

			// test: Unknown database type
			Connections connection = spyHandler.parseConnection(eventData);
			spyHandler.handleSync(eventData, mockSendMessage, connection);
			assertNotNull(errorEx.get());
			if (null == errorEx.get().getMessage() || !errorEx.get().getMessage().startsWith("Unknown database type")) {
				throw errorEx.get();
			}
		}

		@Test
		void testNotSave() throws Exception {
			ConnectionUpdateOperation.TYPE.set(eventData, "sampleType");
			ConnectionUpdateOperation.CONNECTION_ID.set(eventData, "sampleConnectionId");
			ConnectionUpdateOperation.NAME.set(eventData, connName);
			ConnectionUpdateOperation.PDK_HASH.set(eventData, pdkHash);
			ConnectionUpdateOperation.SCHEMA_VERSION.set(eventData, "sampleSchemaVersion");
			ConnectionUpdateOperation.DATABASE_TYPE.set(eventData, DatabaseTypeEnum.MONGODB.getType());
			ConnectionUpdateOperation.PDK_TYPE.set(eventData, "samplePdkType");

			try (MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class);) {
				DatabaseTypeEnum.DatabaseType databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
				connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(clientMongoOperator, pdkHash)).thenReturn(databaseType);
				doNothing().when(spyHandler).downloadPdkFile(any());

				Connections connection = spyHandler.parseConnection(eventData);
				spyHandler.handleSync(eventData, mockSendMessage, connection);
				if (null != errorEx.get()) {
					throw errorEx.get();
				}
				verify(spyHandler, times(1)).updateLoadSchemaFieldStatus(eq(isExternalStorage), any(), eq(ConnectorConstant.LOAD_FIELD_STATUS_FINISHED), any(), any());
			}
		}

		@Test
		void testSave() throws Exception {
			isExternalStorage = true;
			ConnectionUpdateOperation.TYPE.set(eventData, "sampleType");
			ConnectionUpdateOperation.CONNECTION_ID.set(eventData, "sampleConnectionId");
			ConnectionUpdateOperation.NAME.set(eventData, connName);
			ConnectionUpdateOperation.PDK_HASH.set(eventData, pdkHash);
			ConnectionUpdateOperation.SCHEMA_VERSION.set(eventData, "sampleSchemaVersion");
			ConnectionUpdateOperation.DATABASE_TYPE.set(eventData, DatabaseTypeEnum.MONGODB.getType());
			ConnectionUpdateOperation.PDK_TYPE.set(eventData, "samplePdkType");
			ConnectionUpdateOperation.EDIT_TEST.set(eventData, true);
			ConnectionUpdateOperation.IS_EXTERNAL_STORAGE_ID.set(eventData, isExternalStorage);
			ConnectionUpdateOperation.EXTERNAL_STORAGE_ID.set(eventData, "test-externalStorageId");

			try (MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class);) {
				DatabaseTypeEnum.DatabaseType databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
				connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(clientMongoOperator, pdkHash)).thenReturn(databaseType);
				doNothing().when(spyHandler).downloadPdkFile(any());

				Connections connection = spyHandler.parseConnection(eventData);
				spyHandler.handleSync(eventData, mockSendMessage, connection);
				if (null != errorEx.get()) {
					throw errorEx.get();
				}
				verify(spyHandler, times(1)).updateLoadSchemaFieldStatus(eq(isExternalStorage), any(), eq(ConnectorConstant.LOAD_FIELD_STATUS_FINISHED), any(), any());
			}
		}

		@Test
		void testNotPdkType() throws Exception {
			String testType = "rocksdb";
			isExternalStorage = true;
			eventData.put("testType", testType);
			ConnectionUpdateOperation.TYPE.set(eventData, "sampleType");
			ConnectionUpdateOperation.CONNECTION_ID.set(eventData, "sampleConnectionId");
			ConnectionUpdateOperation.NAME.set(eventData, connName);
			ConnectionUpdateOperation.PDK_HASH.set(eventData, pdkHash);
			ConnectionUpdateOperation.SCHEMA_VERSION.set(eventData, "sampleSchemaVersion");
			ConnectionUpdateOperation.DATABASE_TYPE.set(eventData, DatabaseTypeEnum.MONGODB.getType());
			ConnectionUpdateOperation.EDIT_TEST.set(eventData, true);
			ConnectionUpdateOperation.IS_EXTERNAL_STORAGE_ID.set(eventData, isExternalStorage);
			ConnectionUpdateOperation.EXTERNAL_STORAGE_ID.set(eventData, "test-externalStorageId");

			try (MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class);) {
				DatabaseTypeEnum.DatabaseType databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
				connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(clientMongoOperator, pdkHash)).thenReturn(databaseType);
				doNothing().when(spyHandler).downloadPdkFile(any());
				when(spyHandler.getInstance(testType)).thenReturn(mock(TestConnection.class));

				Connections connection = spyHandler.parseConnection(eventData);
				spyHandler.handleSync(eventData, mockSendMessage, connection);
				if (null != errorEx.get()) {
					throw errorEx.get();
				}
				verify(spyHandler, times(1)).handleNoPdkTestConnection(any(), any());
			}
		}
	}

	@Nested
	class TestUpdatePDKConnectionTestResult {

		@Test
		void whenExternalAndNextRetryAndNeedLoadFields() {
			// Create mock objects
			String name = "test-name";
			String collection = "test-collection";
			Long nextRetry = 12345L;

			Query connectionIdQuery = mock(Query.class);
			ConnectionValidateResult validateResult = mock(ConnectionValidateResult.class);
			List<ConnectionValidateResultDetail> validateResultDetails = mock(List.class);
			ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
			TestConnectionHandler spyInstance = spy(TestConnectionHandler.class);
			UnitTestUtils.injectField(TestConnectionHandler.class, spyInstance, "collection", collection);
			UnitTestUtils.injectField(TestConnectionHandler.class, spyInstance, "clientMongoOperator", clientMongoOperator);
			when(clientMongoOperator.update(eq(connectionIdQuery), any(Update.class), eq(collection))).thenAnswer(invocationOnMock -> {
				Update update = invocationOnMock.getArgument(1, Update.class);
				assertNotNull(update);
				Document updateObject = update.getUpdateObject();
				assertNotNull(updateObject);
				updateObject = updateObject.get("$set", Document.class);
				assertNotNull(updateObject);
				assertFalse(updateObject.getBoolean(ConnectionUpdateOperation.LOAD_SCHEMA_FIELD.getFullKey()));
				assertEquals(nextRetry, updateObject.getLong(ConnectionUpdateOperation.ResponseBody.NEXT_RETRY.getFullKey()));
				assertEquals(name, updateObject.getString(ConnectionUpdateOperation.NAME.getFullKey()));
				return null;
			});

			// Set up the necessary data for the test
			when(validateResult.getRetry()).thenReturn(2);
			when(validateResult.getNextRetry()).thenReturn(nextRetry);
			when(validateResult.getStatus()).thenReturn("success");
			when(validateResult.getConnectionOptions()).thenReturn(mock(ConnectionOptions.class));

			// Call the method to be tested
			spyInstance.updatePDKConnectionTestResult(connectionIdQuery, true, false, validateResult, validateResultDetails, true, name);

			// Verify that the update method was called with the correct parameters
			verify(clientMongoOperator, times(1)).update(eq(connectionIdQuery), any(Update.class), eq(collection));
		}


		@Test
		void whenNotExternalAndNextRetryAndNeedLoadFields() {
			boolean isExternal = false;
			String name = null;
			String collection = "test-collection";
			Long nextRetry = null;
			Schema schema = mock(Schema.class); // need load fields

			// Create mock objects
			Query connectionIdQuery = mock(Query.class);
			ConnectionValidateResult validateResult = mock(ConnectionValidateResult.class);
			when(validateResult.getSchema()).thenReturn(schema);
			List<ConnectionValidateResultDetail> validateResultDetails = mock(List.class);
			ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
			TestConnectionHandler spyInstance = spy(TestConnectionHandler.class);
			when(clientMongoOperator.update(eq(connectionIdQuery), any(Update.class), eq(collection))).thenAnswer(invocationOnMock -> {
				Update update = invocationOnMock.getArgument(1, Update.class);
				assertNotNull(update);
				Document updateObject = update.getUpdateObject();
				assertNotNull(updateObject);
				updateObject = updateObject.get("$set", Document.class);
				assertNotNull(updateObject);
				assertTrue(updateObject.getBoolean(ConnectionUpdateOperation.LOAD_SCHEMA_FIELD.getFullKey()));
				assertFalse(updateObject.containsKey(ConnectionUpdateOperation.ResponseBody.NEXT_RETRY.getFullKey()));
				assertFalse(updateObject.containsKey(ConnectionUpdateOperation.NAME.getFullKey()));
				return null;
			});
			UnitTestUtils.injectField(TestConnectionHandler.class, spyInstance, "collection", collection);
			UnitTestUtils.injectField(TestConnectionHandler.class, spyInstance, "clientMongoOperator", clientMongoOperator);

			// Set up the necessary data for the test
			when(validateResult.getRetry()).thenReturn(2);
			when(validateResult.getNextRetry()).thenReturn(nextRetry);
			when(validateResult.getStatus()).thenReturn("success");
			when(validateResult.getConnectionOptions()).thenReturn(mock(ConnectionOptions.class));

			// Call the method to be tested
			spyInstance.updatePDKConnectionTestResult(connectionIdQuery, true, false, validateResult, validateResultDetails, isExternal, name);

			// Verify that the update method was called with the correct parameters
			verify(clientMongoOperator, times(1)).update(eq(connectionIdQuery), any(Update.class), eq(collection));
			verify(clientMongoOperator, times(1)).update(eq(connectionIdQuery), any(Update.class), eq(TestConnectionHandler.COLLECTION_CONNECTION_OPTIONS));
		}
	}

	@Test
	void test() {
		Map<String, Object> event = new HashMap<>();
		Connections connection = new Connections();
		String connName = "test-connection";
		SendMessage<WebSocketEventResult> sendMessage = mock(SendMessage.class);
		Exception ex;

		Logger mockLogger = mock(Logger.class);
		TestConnectionHandler spyHandler = spy(instance);
		ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
		UnitTestUtils.injectField(TestConnectionHandler.class, spyHandler, "clientMongoOperator", clientMongoOperator);
		try (MockedStatic<TestConnectionHandler> testConnectionHandlerMockedStatic = mockStatic(TestConnectionHandler.class, CALLS_REAL_METHODS)) {
			testConnectionHandlerMockedStatic.when(TestConnectionHandler::getLogger).thenReturn(mockLogger);

			// test: not tm status exception
			ex = new RuntimeException("test-available-exception");
			spyHandler.handleError(event, connection, connName, sendMessage, ex);
			verify(spyHandler, times(1)).updateConnectionInvalid(eq(connection));

			// test: tm status exception
			clearInvocations(spyHandler);
			try (MockedStatic<TmUnavailableException> tmUnavailableExceptionMockedStatic = mockStatic(TmUnavailableException.class)) {
				ex = new TmUnavailableException("test-unavailable-exception", "post", null, new ResponseBody());
				final Exception e = ex;
				tmUnavailableExceptionMockedStatic.when(() -> TmUnavailableException.isInstance(e)).thenReturn(true);

				spyHandler.handleError(event, connection, connName, sendMessage, ex);
				verify(spyHandler, times(1)).updateConnectionInvalid(null);
			}

			// test: throws exception
			clearInvocations(spyHandler);
			try (MockedStatic<TmUnavailableException> tmUnavailableExceptionMockedStatic = mockStatic(TmUnavailableException.class)) {
				ex = new TmUnavailableException("test-unavailable-exception", "post", null, new ResponseBody());
				final Exception e = ex;
				tmUnavailableExceptionMockedStatic.when(() -> TmUnavailableException.isInstance(e)).thenThrow(new RuntimeException("x"));

				spyHandler.handleError(event, connection, connName, sendMessage, ex);
				verify(spyHandler, times(1)).updateConnectionInvalid(eq(connection));
			}
		}
	}

	@Test
	void testNeedLoadField() {
		// test: schema is null
		assertFalse(instance.needLoadField(true, null, true));

		// test: schema includes fields
		Schema schemaWithFields = new Schema();
		schemaWithFields.setIncludeFields(true);
		assertFalse(instance.needLoadField(true, schemaWithFields, true));

		// test: everLoadSchema is false
		schemaWithFields.setIncludeFields(false);
		assertTrue(instance.needLoadField(true, schemaWithFields, false));

		// test: updateSchema is false
		assertFalse(instance.needLoadField(false, schemaWithFields, true));

		// test: default case
		assertTrue(instance.needLoadField(true, schemaWithFields, true));
	}

	@Nested
	class TestConnectionTest {

		@Test
		void testGetInstanceForRocksDB() {
			TestConnection testConnection = instance.getInstance("rocksdb");

			assertNotNull(testConnection);
			assertTrue(testConnection instanceof RocksDBTestConnectionImpl);
		}

		@Test
		void testGetInstanceForUnknownType() {

			try {
				instance.getInstance("unknownType");
				fail("Expected ConnectionException was not thrown");
			} catch (ConnectionException e) {
				assertEquals("TestType not found instance 'unknownType'", e.getMessage());
			}
		}
	}
}

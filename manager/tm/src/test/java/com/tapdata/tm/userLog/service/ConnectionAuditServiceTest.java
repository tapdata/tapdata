package com.tapdata.tm.userLog.service;

import com.tapdata.tm.userLog.constant.AuditOutcome;
import com.tapdata.tm.userLog.param.AuditLogParam;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConnectionAuditServiceTest {

	private static final String COLLECTION = "AuditOperationContexts";

	@Test
	void shouldKeepLoadSchemaContextAfterSuccessfulPrerequisiteConnectionTest() {
		MongoTemplate mongoTemplate = mock(MongoTemplate.class);
		UserLogService userLogService = mock(UserLogService.class);
		ConnectionAuditService service = new ConnectionAuditService(mongoTemplate, userLogService);
		when(mongoTemplate.findOne(any(Query.class), eq(Document.class), eq(COLLECTION)))
				.thenReturn(context(ConnectionAuditService.ACTION_LOAD_SCHEMA));

		service.completeConnectionTest("connection-id", "browser-session", true);

		verify(mongoTemplate, never()).findAndRemove(any(Query.class), eq(Document.class), eq(COLLECTION));
		verify(userLogService, never()).addAuditLog(any(AuditLogParam.class));
	}

	@Test
	void shouldConsumeAndRecordFailedConnectionTest() {
		MongoTemplate mongoTemplate = mock(MongoTemplate.class);
		UserLogService userLogService = mock(UserLogService.class);
		ConnectionAuditService service = new ConnectionAuditService(mongoTemplate, userLogService);
		Document context = context(ConnectionAuditService.ACTION_TEST_CONNECTION);
		when(mongoTemplate.findOne(any(Query.class), eq(Document.class), eq(COLLECTION))).thenReturn(context);
		when(mongoTemplate.findAndRemove(any(Query.class), eq(Document.class), eq(COLLECTION))).thenReturn(context);

		service.completeConnectionTest("connection-id", "browser-session", false);

		ArgumentCaptor<AuditLogParam> captor = ArgumentCaptor.forClass(AuditLogParam.class);
		verify(userLogService).addAuditLog(captor.capture());
		assertEquals(AuditOutcome.FAILURE, captor.getValue().getOutcome());
		assertEquals("connection_test_failed", captor.getValue().getFailureReason());
		assertEquals("203.0.113.8", captor.getValue().getSourceIp());
	}

	@Test
	void shouldRecordSchemaLoadAtTerminalStatus() {
		MongoTemplate mongoTemplate = mock(MongoTemplate.class);
		UserLogService userLogService = mock(UserLogService.class);
		ConnectionAuditService service = new ConnectionAuditService(mongoTemplate, userLogService);
		when(mongoTemplate.findAndRemove(any(Query.class), eq(Document.class), eq(COLLECTION)))
				.thenReturn(context(ConnectionAuditService.ACTION_LOAD_SCHEMA));

		service.completeSchemaLoad("connection-id", "finished");

		ArgumentCaptor<AuditLogParam> captor = ArgumentCaptor.forClass(AuditLogParam.class);
		verify(userLogService).addAuditLog(captor.capture());
		assertEquals(AuditOutcome.SUCCESS, captor.getValue().getOutcome());
		assertEquals(ConnectionAuditService.ACTION_LOAD_SCHEMA, captor.getValue().getAction());
	}

	private Document context(String action) {
		return new Document("_id", new ObjectId())
				.append("action", action)
				.append("userId", "user-id")
				.append("customerId", "customer-id")
				.append("username", "user@example.com")
				.append("sourceIp", "203.0.113.8")
				.append("objectName", "mysql");
	}
}

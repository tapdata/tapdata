package com.tapdata.tm.modules.service;

import com.tapdata.tm.audit.AuditAction;
import com.tapdata.tm.audit.AuditOperationFinisher;
import com.tapdata.tm.audit.AuditOutcome;
import com.tapdata.tm.modules.controller.ModulesController;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.repository.ModulesRepository;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.config.security.UserDetail;
import com.mongodb.client.result.UpdateResult;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.mockito.Mockito.*;

class PublishAuditCorrelationTest {
	@Test
	void published_configuration_and_operation_are_written_together_and_replace_old_owner() {
		ModulesRepository repository = mock(ModulesRepository.class);
		MongoTemplate mongo = mock(MongoTemplate.class);
		AuditOperationFinisher finisher = mock(AuditOperationFinisher.class);
		UserDetail user = mock(UserDetail.class);
		ModulesService service = new ModulesService(repository);
		ReflectionTestUtils.setField(service, "mongoOperations", mongo);
		ReflectionTestUtils.setField(service, "auditOperationFinisher", finisher);
		ObjectId id = new ObjectId();
		ModulesDto dto = new ModulesDto();
		dto.setId(id);
		dto.setName("api");
		dto.setStatus("active");
		when(repository.buildUpdateSet(any(ModulesEntity.class), same(user))).thenAnswer(invocation -> {
			ModulesEntity entity = invocation.getArgument(0);
			return new Update().set("status", entity.getStatus())
					.set("auditPublishOperationId", entity.getAuditPublishOperationId());
		});
		ModulesEntity old = new ModulesEntity();
		old.setAuditPublishOperationId("old-operation");
		when(mongo.findAndModify(any(Query.class), any(Update.class), any(FindAndModifyOptions.class),
				eq(ModulesEntity.class))).thenReturn(old);
		when(repository.findOne(any(Query.class), same(user))).thenReturn(Optional.empty());
		ReflectionTestUtils.invokeMethod(service, "savePublishedConfiguration", dto, user, "new-operation", true);
		ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
		verify(mongo).findAndModify(any(Query.class), update.capture(), any(FindAndModifyOptions.class), eq(ModulesEntity.class));
		org.bson.Document fields = update.getValue().getUpdateObject().get("$set", org.bson.Document.class);
		assertEquals("active", fields.getString("status"));
		assertEquals("new-operation", fields.getString("auditPublishOperationId"));
		verify(finisher).finishOperation(eq("api"), eq(id.toHexString()), eq("api"), eq(AuditAction.API_PUBLISH),
				eq("old-operation"), eq(AuditOutcome.FAILED), contains("Superseded"));
	}
	@Test
	void publish_report_forwards_action_operation_and_each_result() {
		ModulesService service = mock(ModulesService.class);
		AuditOperationFinisher finisher = mock(AuditOperationFinisher.class);
		ReflectionTestUtils.setField(service, "auditOperationFinisher", finisher);
		MongoTemplate mongo = mock(MongoTemplate.class);
		ReflectionTestUtils.setField(service, "mongoOperations", mongo);
		when(mongo.updateFirst(any(Query.class), any(Update.class), eq(ModulesEntity.class)))
				.thenReturn(UpdateResult.acknowledged(1L, 1L, null));
		Map<String, String> results = Map.of("bad", "config-error", "good", "");
		doCallRealMethod().when(service).updatePublishMsg(results, "publish-v1");
		service.updatePublishMsg(results, "publish-v1");
		verify(finisher).finishOperation("api", "bad", null, AuditAction.API_PUBLISH,
				"publish-v1", AuditOutcome.FAILED, "config-error");
		verify(finisher).finishOperation("api", "good", null, AuditAction.API_PUBLISH,
				"publish-v1", AuditOutcome.SUCCESS, "");
		verifyNoMoreInteractions(finisher);
	}

	@Test
	void stale_report_cannot_mark_new_configuration_successful() {
		ModulesService service = mock(ModulesService.class);
		AuditOperationFinisher finisher = mock(AuditOperationFinisher.class);
		MongoTemplate mongo = mock(MongoTemplate.class);
		ReflectionTestUtils.setField(service, "auditOperationFinisher", finisher);
		ReflectionTestUtils.setField(service, "mongoOperations", mongo);
		when(mongo.updateFirst(any(Query.class), any(Update.class), eq(ModulesEntity.class)))
				.thenReturn(UpdateResult.acknowledged(0L, 0L, null));
		Map<String, String> results = Map.of("507f1f77bcf86cd799439011", "");
		doCallRealMethod().when(service).updatePublishMsg(results, "old-operation");
		service.updatePublishMsg(results, "old-operation");
		verify(finisher).finishOperation(eq("api"), eq("507f1f77bcf86cd799439011"), isNull(),
				eq(AuditAction.API_PUBLISH), eq("old-operation"), eq(AuditOutcome.FAILED), contains("Superseded"));
	}

	@Test
	void old_reports_remain_uncorrelated_instead_of_using_the_reporting_requests_id() {
		ModulesService service = mock(ModulesService.class);
		Map<String, String> results = Map.of("api", "");
		doCallRealMethod().when(service).updatePublishMsg(results);
		service.updatePublishMsg(results);
		verify(service).updatePublishMsg(results, null);
	}

	@Test
	void controller_preserves_the_explicit_operation_parameter() {
		ModulesService service = mock(ModulesService.class);
		ModulesController controller = new ModulesController();
		ReflectionTestUtils.setField(controller, "modulesService", service);
		Map<String, String> results = Map.of("api", "");
		controller.updatePublishStatus(results, "publish-v1");
		verify(service).updatePublishMsg(results, "publish-v1");
	}
}

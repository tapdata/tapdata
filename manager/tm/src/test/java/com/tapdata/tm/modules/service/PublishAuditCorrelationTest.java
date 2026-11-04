package com.tapdata.tm.modules.service;

import com.tapdata.tm.audit.AuditAction;
import com.tapdata.tm.audit.AuditOperationFinisher;
import com.tapdata.tm.audit.AuditOutcome;
import com.tapdata.tm.modules.controller.ModulesController;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.mockito.Mockito.*;

class PublishAuditCorrelationTest {
	@Test
	void publish_report_forwards_action_operation_and_each_result() {
		ModulesService service = mock(ModulesService.class);
		AuditOperationFinisher finisher = mock(AuditOperationFinisher.class);
		ReflectionTestUtils.setField(service, "auditOperationFinisher", finisher);
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

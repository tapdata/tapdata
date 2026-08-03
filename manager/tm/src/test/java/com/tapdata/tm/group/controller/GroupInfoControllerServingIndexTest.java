package com.tapdata.tm.group.controller;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.service.GroupInfoService;
import com.tapdata.tm.module.dto.ServingIndexImportResult;
import com.tapdata.tm.module.dto.ServingIndexLandingReport;
import com.tapdata.tm.module.dto.ServingIndexLandingSummary;
import com.tapdata.tm.module.dto.ServingIndexPlanDiffs;
import com.tapdata.tm.module.dto.ServingIndexPreviewResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * P4-1 · 索引腿的两个端点（TAP-12057，方案 §3.5）。
 *
 * <p>这里钉的是<b>对 worker 的契约</b>，不是业务逻辑（那在 {@code ServingIndexPlanDiffsTest} /
 * {@code GroupInfoServiceServingIndexLegTest}）：</p>
 * <ul>
 *   <li><b>路径与 multipart 形状</b>——{@code preview-resource.sh}/{@code import-resource.sh} 里
 *       是硬编码的 URL，改这里就是改 CICD 的对接面（P4-2 只加一条分支）。</li>
 *   <li><b>出问题时 {@code code} 必须不是 {@code ok}</b>——那两个通用脚本只看这一个字段决定这一步红不红。
 *       漏接等于「索引没建成，流水线一路绿」，正是本工单要消灭的静默的谎。</li>
 * </ul>
 */
class GroupInfoControllerServingIndexTest {

	private GroupInfoService groupInfoService;
	private GroupInfoController controller;
	private UserDetail user;
	private MultipartFile file;

	@BeforeEach
	void setUp() {
		groupInfoService = mock(GroupInfoService.class);
		user = mock(UserDetail.class);
		file = mock(MultipartFile.class);
		controller = spy(new GroupInfoController());
		ReflectionTestUtils.setField(controller, "groupInfoService", groupInfoService);
		doReturn(user).when(controller).getLoginUser();
	}

	private static ServingIndexLandingReport reportWithProblem() {
		ServingIndexLandingReport report = new ServingIndexLandingReport();
		ServingIndexLandingSummary summary = new ServingIndexLandingSummary();
		summary.getProblems().add("fdm(64b7e1f4c9e77a0001aa0001).MDM_CUSTOMER: CUSTOMER_ID_-1: "
				+ "reported created but absent on re-read");
		report.setSummary(summary);
		return report;
	}

	@Test
	@DisplayName("preview 端点挂在 /preview/indexes 上、收 multipart —— worker 脚本硬编码了这个路径")
	void previewIsMappedWhereTheWorkerCallsIt() throws NoSuchMethodException {
		Method method = GroupInfoController.class.getMethod("previewIndexes", MultipartFile.class);
		PostMapping mapping = method.getAnnotation(PostMapping.class);

		assertNotNull(mapping);
		assertTrue(Arrays.asList(mapping.path()).contains("/preview/indexes"));
		assertTrue(Arrays.asList(mapping.consumes()).contains(MediaType.MULTIPART_FORM_DATA_VALUE));
		assertTrue(Arrays.asList(GroupInfoController.class.getAnnotation(RequestMapping.class).value())
				.contains("/api/groupInfo"));
	}

	@Test
	@DisplayName("import 端点挂在 /import/indexes 上、收 multipart")
	void importIsMappedWhereTheWorkerCallsIt() throws NoSuchMethodException {
		Method method = GroupInfoController.class.getMethod("importIndexes", MultipartFile.class);
		PostMapping mapping = method.getAnnotation(PostMapping.class);

		assertNotNull(mapping);
		assertTrue(Arrays.asList(mapping.path()).contains("/import/indexes"));
		assertTrue(Arrays.asList(mapping.consumes()).contains(MediaType.MULTIPART_FORM_DATA_VALUE));
	}

	@Test
	@DisplayName("preview 干净时 code = ok，计划表原样回给 worker")
	void previewPassesThroughWhenClean() throws IOException {
		ServingIndexPreviewResult result = ServingIndexPlanDiffs.preview(new ServingIndexLandingReport());
		when(groupInfoService.previewIndexes(any(MultipartFile.class), any(UserDetail.class))).thenReturn(result);

		ResponseMessage<ServingIndexPreviewResult> response = controller.previewIndexes(file);

		assertEquals(ResponseMessage.OK, response.getCode());
		assertSame(result, response.getData());
	}

	@Test
	@DisplayName("preview 发现问题时 code 转成失败码，且 data 里的报告仍在")
	void previewFlagsProblems() throws IOException {
		ServingIndexPreviewResult result = ServingIndexPlanDiffs.preview(reportWithProblem());
		when(groupInfoService.previewIndexes(any(MultipartFile.class), any(UserDetail.class))).thenReturn(result);

		ResponseMessage<ServingIndexPreviewResult> response = controller.previewIndexes(file);

		assertEquals(ServingIndexPlanDiffs.ERROR_CODE, response.getCode());
		assertTrue(response.getMessage().contains("CUSTOMER_ID_-1"));
		assertSame(result, response.getData(), "失败也要把报告带回去——CI 日志里就这一份现场");
	}

	@Test
	@DisplayName("import 有索引没建成时让这一步红——脚本只看 code，不看报告")
	void importFlagsProblems() throws IOException {
		ServingIndexImportResult result = new ServingIndexImportResult();
		result.setReport(reportWithProblem());
		when(groupInfoService.importIndexes(any(MultipartFile.class), any(UserDetail.class))).thenReturn(result);

		ResponseMessage<ServingIndexImportResult> response = controller.importIndexes(file);

		assertEquals(ServingIndexPlanDiffs.ERROR_CODE, response.getCode());
		assertTrue(response.getMessage().contains("absent on re-read"));
	}

	@Test
	@DisplayName("import 全绿时 code = ok")
	void importPassesThroughWhenClean() throws IOException {
		ServingIndexImportResult result = new ServingIndexImportResult();
		result.setReport(new ServingIndexLandingReport());
		when(groupInfoService.importIndexes(any(MultipartFile.class), any(UserDetail.class))).thenReturn(result);

		ResponseMessage<ServingIndexImportResult> response = controller.importIndexes(file);

		assertEquals(ResponseMessage.OK, response.getCode());
		assertTrue(response.getData().getDiff().getAdd().isEmpty());
	}

	@Test
	@DisplayName("落地服务没给报告时不炸（防御：null 报告不该把整条腿打挂）")
	void toleratesMissingReport() throws IOException {
		ServingIndexImportResult result = new ServingIndexImportResult();
		when(groupInfoService.importIndexes(any(MultipartFile.class), any(UserDetail.class))).thenReturn(result);

		ResponseMessage<ServingIndexImportResult> response = controller.importIndexes(file);

		assertEquals(ResponseMessage.OK, response.getCode());
		assertEquals(Collections.emptyList(), response.getData().getDiff().getUpdate());
	}
}

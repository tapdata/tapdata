package com.tapdata.tm.group.service;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.group.handler.ResourceHandler;
import com.tapdata.tm.group.handler.ResourceHandlerRegistry;
import com.tapdata.tm.group.repostitory.GroupInfoRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.ServingIndex;
import com.tapdata.tm.module.dto.ServingIndexField;
import com.tapdata.tm.module.dto.ServingIndexImportResult;
import com.tapdata.tm.module.dto.ServingIndexLandingPlan;
import com.tapdata.tm.module.dto.ServingIndexLandingReport;
import com.tapdata.tm.module.dto.ServingIndexLandingTarget;
import com.tapdata.tm.module.dto.ServingIndexLandingWorkList;
import com.tapdata.tm.module.dto.ServingIndexPlanDiffs;
import com.tapdata.tm.module.dto.ServingIndexPlanRow;
import com.tapdata.tm.module.dto.ServingIndexPreviewResult;
import com.tapdata.tm.module.dto.ServingIndexReportEntry;
import com.tapdata.tm.module.dto.ServingIndexTargetOutcome;
import com.tapdata.tm.module.dto.ServingIndexTargetReport;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.servingindex.ServingIndexLandingService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * P4-1 · CICD 的<b>索引腿</b>（{@code /preview/indexes} 与 {@code /import/indexes} 的服务侧）。
 * TAP-12057（方案 §3.5）。
 *
 * <p>这一腿与 apis 腿的关键差别，也是本类要钉住的两件事：</p>
 * <ol>
 *   <li><b>不看 Module diff</b>。apis 腿只对「有变更的 Module」落地（{@code toImport} 过滤），
 *       于是「API 一个字没改、但目标库的索引被人删了」这种漂移永远补不回来。索引腿的输入是
 *       <b>包里的声明 + conMap</b>，与 Module 本身有没有 diff 无关——独立成腿的意义正在于此。</li>
 *   <li><b>不写 Module</b>。声明落库是 apis 腿的职责（部署矩阵里索引腿排在 apis 之后），
 *       索引腿只把索引建到用户库。preview 更是连索引都不建。</li>
 * </ol>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GroupInfoServiceServingIndexLegTest {

	private static final String CONNECTION_ID = "64b7e1f4c9e77a0001aa0001";

	@Mock
	private GroupInfoRepository groupInfoRepository;
	@Mock
	private ResourceHandlerRegistry resourceHandlerRegistry;
	@Mock
	private ModulesService modulesService;
	@Mock
	private com.tapdata.tm.ds.service.impl.DataSourceService dataSourceService;
	@Mock
	private MetadataInstancesService metadataInstancesService;
	@Mock
	private ServingIndexLandingService servingIndexLandingService;

	private GroupInfoService groupInfoService;
	private UserDetail user;
	private ModulesDto packagedModule;
	private DataSourceConnectionDto targetConnection;

	@BeforeEach
	void setUp() {
		groupInfoService = new GroupInfoService(groupInfoRepository);
		ReflectionTestUtils.setField(groupInfoService, "resourceHandlerRegistry", resourceHandlerRegistry);
		ReflectionTestUtils.setField(groupInfoService, "modulesService", modulesService);
		ReflectionTestUtils.setField(groupInfoService, "dataSourceService", dataSourceService);
		ReflectionTestUtils.setField(groupInfoService, "metadataInstancesService", metadataInstancesService);
		ReflectionTestUtils.setField(groupInfoService, "servingIndexLandingService", servingIndexLandingService);

		user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
				"accessCode", false, false, false, false,
				Collections.singletonList(new SimpleGrantedAuthority("role")));

		packagedModule = new ModulesDto();
		packagedModule.setId(new ObjectId());
		packagedModule.setName("customer");
		packagedModule.setApiType("defaultApi");
		packagedModule.setConnectionId(CONNECTION_ID);
		packagedModule.setTableName("MDM_CUSTOMER");
		packagedModule.setServingIndexes(new ArrayList<>(Collections.singletonList(
				new ServingIndex("CUSTOMER_ID_1", false,
						new ArrayList<>(Collections.singletonList(new ServingIndexField("CUSTOMER_ID", true)))))));

		targetConnection = new DataSourceConnectionDto();
		targetConnection.setId(new ObjectId(CONNECTION_ID));
		targetConnection.setName("fdm");
	}

	private Map<String, List<TaskUpAndLoadDto>> payloads() {
		TaskUpAndLoadDto item = new TaskUpAndLoadDto(GroupConstants.COLLECTION_MODULES,
				JsonUtil.toJsonUseJackson(packagedModule));
		return Collections.singletonMap("Module.json", new ArrayList<>(Collections.singletonList(item)));
	}

	@SuppressWarnings("unchecked")
	private void wireHandlers() {
		ResourceHandler moduleHandler = mock(ResourceHandler.class);
		org.mockito.Mockito.doAnswer(invocation -> {
			Map<String, Object> resourceMap = invocation.getArgument(1);
			resourceMap.put(packagedModule.getId().toHexString(), packagedModule);
			return null;
		}).when(moduleHandler).collectPayload(anyList(), any(Map.class), anyList());
		org.mockito.Mockito.doAnswer(invocation -> {
			Map<ResourceType, Map<String, ?>> resourceMap = invocation.getArgument(1);
			((Map<String, Object>) resourceMap.computeIfAbsent(ResourceType.CONNECTION, k -> new java.util.HashMap<>()))
					.put(CONNECTION_ID, targetConnection);
			return null;
		}).when(moduleHandler).collectPayloadRelatedResources(any(Map.class), any(Map.class), any(Map.class),
				any(UserDetail.class));

		when(resourceHandlerRegistry.getHandler(ResourceType.MODULE)).thenReturn(moduleHandler);
		when(resourceHandlerRegistry.getAllHandlers()).thenReturn(Collections.singletonList(moduleHandler));
		when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(Collections.singletonList(targetConnection));
		// 目标环境里已经有一份一模一样的 Module（apis 腿这一轮 diff 为空）——索引腿照跑不误。
		when(modulesService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(Collections.singletonList(packagedModule));
	}

	private ServingIndexLandingReport reportWithOneCreate() {
		ServingIndexLandingReport report = new ServingIndexLandingReport();
		ServingIndexTargetReport target = new ServingIndexTargetReport();
		target.setConnectionId(CONNECTION_ID);
		target.setConnectionName("fdm");
		target.setTableName("MDM_CUSTOMER");
		report.getTargets().add(target);
		return report;
	}

	/** 同上，但「将创建」桶里真放一条——手工语句是逐条 create 生成的，空桶测不出接线。 */
	private ServingIndexLandingReport reportPlanningOneIndex() {
		ServingIndexLandingReport report = reportWithOneCreate();
		ServingIndexReportEntry entry = new ServingIndexReportEntry();
		entry.setName("CUSTOMER_ID_1");
		entry.setFields(new ArrayList<>(Collections.singletonList(new ServingIndexField("CUSTOMER_ID", true))));
		report.getTargets().get(0).getCreate().add(entry);
		return report;
	}

	private ServingIndexLandingWorkList workWithOneCreated() {
		ServingIndexLandingTarget target = new ServingIndexLandingTarget(targetConnection, "MDM_CUSTOMER");
		ServingIndex created = new ServingIndex("CUSTOMER_ID_1", false,
				new ArrayList<>(Collections.singletonList(new ServingIndexField("CUSTOMER_ID", true))));
		target.getDeclared().add(created);
		ServingIndexLandingPlan plan = new ServingIndexLandingPlan();
		plan.getCreate().add(created);
		ServingIndexTargetOutcome outcome = new ServingIndexTargetOutcome();
		outcome.setConnectionId(CONNECTION_ID);
		outcome.setConnectionName("fdm");
		outcome.setTableName("MDM_CUSTOMER");
		outcome.setPlan(plan);
		outcome.getCreated().add("CUSTOMER_ID_1");

		ServingIndexLandingWorkList work = new ServingIndexLandingWorkList();
		work.getTargets().add(target);
		work.getOutcomes().add(outcome);
		return work;
	}

	@Test
	@DisplayName("preview：把包里的声明与真 conMap 交给 dry-run，Module 与库里一致时照样跑")
	@SuppressWarnings("unchecked")
	void previewRunsDryRunRegardlessOfModuleDiff() {
		wireHandlers();
		when(servingIndexLandingService.preview(anyList(), any(Map.class), any(UserDetail.class)))
				.thenReturn(reportWithOneCreate());

		ServingIndexPreviewResult result = groupInfoService.previewIndexes(payloads(), user);

		ArgumentCaptor<List<ModulesDto>> modulesCaptor = ArgumentCaptor.forClass(List.class);
		ArgumentCaptor<Map<String, DataSourceConnectionDto>> conMapCaptor = ArgumentCaptor.forClass(Map.class);
		verify(servingIndexLandingService).preview(modulesCaptor.capture(), conMapCaptor.capture(), eq(user));

		assertEquals(1, modulesCaptor.getValue().size(),
				"索引腿不看 Module diff：目标环境已有同一份 Module，声明照样要参与比对");
		assertSame(packagedModule, modulesCaptor.getValue().get(0));
		assertSame(targetConnection, conMapCaptor.getValue().get(CONNECTION_ID),
				"conMap 解析错 = 索引建到别人的库（ADR-0002）");
		assertSame(reportWithOneCreate().getClass(), result.getReport().getClass());
	}

	@Test
	@DisplayName("preview 是只读的：不导 Module、不落地索引")
	@SuppressWarnings("unchecked")
	void previewNeverWrites() {
		wireHandlers();
		when(servingIndexLandingService.preview(anyList(), any(Map.class), any(UserDetail.class)))
				.thenReturn(new ServingIndexLandingReport());

		groupInfoService.previewIndexes(payloads(), user);

		verify(modulesService, never()).batchImport(anyList(), any(UserDetail.class), any(), any(Map.class), any());
		verify(servingIndexLandingService, never()).landAfterImport(anyList(), any(Map.class), any(UserDetail.class));
	}

	@Test
	@DisplayName("import：委派真跑，计划表只列真建成的那条")
	@SuppressWarnings("unchecked")
	void importDelegatesToLandingAndReportsWhatWasCreated() {
		wireHandlers();
		when(servingIndexLandingService.landAfterImport(anyList(), any(Map.class), any(UserDetail.class)))
				.thenReturn(workWithOneCreated());

		ServingIndexImportResult result = groupInfoService.importIndexes(payloads(), user);

		ArgumentCaptor<List<ModulesDto>> modulesCaptor = ArgumentCaptor.forClass(List.class);
		verify(servingIndexLandingService).landAfterImport(modulesCaptor.capture(), any(Map.class), eq(user));
		assertSame(packagedModule, modulesCaptor.getValue().get(0));

		assertEquals(1, result.getDiff().getAdd().size());
		assertEquals("CUSTOMER_ID_1", result.getDiff().getAdd().get(0).getName());
		assertEquals("CUSTOMER_ID:1", result.getDiff().getAdd().get(0).getKeys());
		assertTrue(result.getDiff().getDelete().isEmpty(), "只加不删（ADR-0005/0008）");
	}

	@Test
	@DisplayName("import 不写 Module：声明落库是 apis 腿的事，索引腿只动用户库")
	@SuppressWarnings("unchecked")
	void importNeverWritesModules() {
		wireHandlers();
		when(servingIndexLandingService.landAfterImport(anyList(), any(Map.class), any(UserDetail.class)))
				.thenReturn(new ServingIndexLandingWorkList());

		groupInfoService.importIndexes(payloads(), user);

		verify(modulesService, never()).batchImport(anyList(), any(UserDetail.class), any(), any(Map.class), any());
	}

	@Test
	@DisplayName("preview · 首次部署：目标环境还没有这个连接 → 不报错，按包内声明出计划")
	@SuppressWarnings("unchecked")
	void previewPlansDeclaredIndexesWhenTargetConnectionHasNotLandedYet() {
		wireHandlers();
		// connections 腿排在索引腿之后：preview 那一刻目标环境一个连接都没有（首次部署的常态）
		when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(Collections.emptyList());

		ServingIndexPreviewResult result = groupInfoService.previewIndexes(payloads(), user);

		verify(servingIndexLandingService, never()).preview(anyList(), any(Map.class), any(UserDetail.class));
		assertEquals(1, result.getAdd().size(),
				"计划表为空 → has_changes=false → 这条腿进不了 matrix → 索引永远建不上");
		ServingIndexPlanRow row = result.getAdd().get(0);
		assertEquals("CUSTOMER_ID:1", row.getKeys());
		assertEquals("fdm", row.getConnection(), "取包里那个连接的名字，落地后同名同 id 就是它");
		assertNull(ServingIndexPlanDiffs.problemOf(result.getReport()),
				"首次部署不该红——连接由排在前面的 connections 腿落地（ADR-0032）");
	}

	@Test
	@DisplayName("import 不吃这套宽容：连接解不出仍交给落地服务，由它照常报红")
	@SuppressWarnings("unchecked")
	void importStillDelegatesWhenTargetConnectionMissing() {
		wireHandlers();
		when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(Collections.emptyList());
		when(servingIndexLandingService.landAfterImport(anyList(), any(Map.class), any(UserDetail.class)))
				.thenReturn(new ServingIndexLandingWorkList());

		groupInfoService.importIndexes(payloads(), user);

		ArgumentCaptor<List<ModulesDto>> modulesCaptor = ArgumentCaptor.forClass(List.class);
		verify(servingIndexLandingService).landAfterImport(modulesCaptor.capture(), any(Map.class), eq(user));
		assertEquals(1, modulesCaptor.getValue().size(),
				"落地那一刻 connections 腿已经跑完，连接还解不出就是真没建成，必须红（ADR-0030）");
	}

	@Test
	@DisplayName("preview · 真实比对路径：MongoDB 目标的计划行配一条手工建索引语句")
	@SuppressWarnings("unchecked")
	void previewAttachesManualCommandForMongoTarget() {
		targetConnection.setDatabase_type("MongoDB");
		wireHandlers();
		when(servingIndexLandingService.preview(anyList(), any(Map.class), any(UserDetail.class)))
				.thenReturn(reportPlanningOneIndex());

		ServingIndexPreviewResult result = groupInfoService.previewIndexes(payloads(), user);

		assertEquals(1, result.getAdd().size());
		assertEquals(Collections.singletonList(
						"db.MDM_CUSTOMER.createIndex({ \"CUSTOMER_ID\": 1 }, "
								+ "{ name: \"CUSTOMER_ID_1\", background: true })"),
				result.getCommands(),
				"类型表按目标连接 id 建键（报告里 target 的 connectionId 就是它）——改按连接名或包内 id "
						+ "建键都查不到，语句会静默地一条都不出，而计划表照常显示，没人察觉");
	}

	@Test
	@DisplayName("preview · 非 MongoDB 目标不出语句：宁可不给，也不给一条可能跑错的")
	@SuppressWarnings("unchecked")
	void previewOmitsManualCommandForNonMongoTarget() {
		targetConnection.setDatabase_type("MySQL");
		wireHandlers();
		when(servingIndexLandingService.preview(anyList(), any(Map.class), any(UserDetail.class)))
				.thenReturn(reportPlanningOneIndex());

		ServingIndexPreviewResult result = groupInfoService.previewIndexes(payloads(), user);

		assertEquals(1, result.getAdd().size(), "计划行照出——不出的只是语句");
		assertTrue(result.getCommands().isEmpty(),
				"其它数据源建索引语法各异，凭空生成一条可能跑错的语句比不给更糟");
	}

	@Test
	@DisplayName("preview · 首次部署：连接还没落地，语句也照出（按包里那个连接的类型）")
	@SuppressWarnings("unchecked")
	void previewAttachesManualCommandOnThePendingPath() {
		targetConnection.setDatabase_type("MongoDB");
		wireHandlers();
		// 首次部署：connections 腿排在索引腿之后，此刻目标环境一个连接都没有（ADR-0032）
		when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(Collections.emptyList());

		ServingIndexPreviewResult result = groupInfoService.previewIndexes(payloads(), user);

		verify(servingIndexLandingService, never()).preview(anyList(), any(Map.class), any(UserDetail.class));
		assertEquals(Collections.singletonList(
						"db.MDM_CUSTOMER.createIndex({ \"CUSTOMER_ID\": 1 }, "
								+ "{ name: \"CUSTOMER_ID_1\", background: true })"),
				result.getCommands(),
				"待落地那条路走的是 split.getPendingCommands()，与真实比对路径各接各的——"
						+ "少接一条，首次部署（最典型的场景）就恰好没有语句");
	}

	@Test
	@DisplayName("包里没有 Module：两条腿都给空计划表，且一次都不触发落地")
	@SuppressWarnings("unchecked")
	void emptyPackageYieldsEmptyPlan() {
		when(resourceHandlerRegistry.getHandler(ResourceType.MODULE)).thenReturn(null);
		Map<String, List<TaskUpAndLoadDto>> empty = Collections.emptyMap();

		assertTrue(groupInfoService.previewIndexes(empty, user).getAdd().isEmpty());
		assertTrue(groupInfoService.importIndexes(empty, user).getDiff().getAdd().isEmpty());

		verify(servingIndexLandingService, never()).preview(anyList(), any(Map.class), any(UserDetail.class));
		verify(servingIndexLandingService, never()).landAfterImport(anyList(), any(Map.class), any(UserDetail.class));
	}
}

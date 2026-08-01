package com.tapdata.tm.group.service;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.ImportModeEnum;
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
import org.mockito.InOrder;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * P3-1 接线：API 导入落完 Module 之后触发索引落地（TAP-12057，方案 §3.4）。
 *
 * <p>时序是这条链的要害——落地必须发生在 {@code dataSourceService.batchImport}（连接已在目标环境就位、
 * conMap 已建立）与 {@code modulesService.batchImport}（声明已落库）<b>之后</b>，且拿到的必须是那张
 * <b>真的 conMap</b>：连接解析错了就等于把索引建到别的库（<b>ADR-0002</b> 错库红线）。故此处不只验"被调用"，
 * 还验调用顺序与 conMap 内容。</p>
 *
 * <p>刻意独立成类、不进 {@code GroupInfoServiceTest}：那个类已有上千行与本特性无关的用例。</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GroupInfoServiceServingIndexLandingTest {

	private static final String CONNECTION_ID = "64b7e1f4c9e77a0001aa0001";

	@Mock
	private GroupInfoRepository groupInfoRepository;
	@Mock
	private GroupInfoRecordService groupInfoRecordService;
	@Mock
	private ResourceHandlerRegistry resourceHandlerRegistry;
	@Mock
	private ModulesService modulesService;
	@Mock
	private com.tapdata.tm.ds.service.impl.DataSourceService dataSourceService;
	@Mock
	private MetadataInstancesService metadataInstancesService;
	@Mock
	private com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService metadataDefinitionService;
	@Mock
	private ServingIndexLandingService servingIndexLandingService;

	private GroupInfoService groupInfoService;
	private UserDetail user;
	private ModulesDto importedModule;
	private DataSourceConnectionDto targetConnection;

	@BeforeEach
	void setUp() {
		groupInfoService = new GroupInfoService(groupInfoRepository);
		ReflectionTestUtils.setField(groupInfoService, "groupInfoRecordService", groupInfoRecordService);
		ReflectionTestUtils.setField(groupInfoService, "resourceHandlerRegistry", resourceHandlerRegistry);
		ReflectionTestUtils.setField(groupInfoService, "modulesService", modulesService);
		ReflectionTestUtils.setField(groupInfoService, "dataSourceService", dataSourceService);
		ReflectionTestUtils.setField(groupInfoService, "metadataInstancesService", metadataInstancesService);
		ReflectionTestUtils.setField(groupInfoService, "metadataDefinitionService", metadataDefinitionService);
		ReflectionTestUtils.setField(groupInfoService, "servingIndexLandingService", servingIndexLandingService);

		user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
				"accessCode", false, false, false, false,
				Collections.singletonList(new SimpleGrantedAuthority("role")));

		importedModule = new ModulesDto();
		importedModule.setId(new ObjectId());
		importedModule.setName("查客户");
		importedModule.setApiType("defaultApi");
		importedModule.setConnectionId(CONNECTION_ID);
		importedModule.setTableName("CUSTOMER");
		importedModule.setServingIndexes(new ArrayList<>(Collections.singletonList(
				new ServingIndex("a_1", null,
						new ArrayList<>(Collections.singletonList(new ServingIndexField("a", true)))))));

		targetConnection = new DataSourceConnectionDto();
		targetConnection.setId(new ObjectId(CONNECTION_ID));
		targetConnection.setName("MDM-生产库");
	}

	/** payloads 里放一份 Module.json，使 buildApiDiff 把它判成「新增」。 */
	private Map<String, List<TaskUpAndLoadDto>> payloads() {
		TaskUpAndLoadDto item = new TaskUpAndLoadDto(GroupConstants.COLLECTION_MODULES,
				JsonUtil.toJsonUseJackson(importedModule));
		return Collections.singletonMap("Module.json", new ArrayList<>(Collections.singletonList(item)));
	}

	@SuppressWarnings("unchecked")
	private void wireHandlers() {
		ResourceHandler moduleHandler = mock(ResourceHandler.class);
		// collectPayload：把导入包里的 Module 放进 resourceMap（真实 handler 的行为）
		org.mockito.Mockito.doAnswer(invocation -> {
			Map<String, Object> resourceMap = invocation.getArgument(1);
			resourceMap.put(importedModule.getId().toHexString(), importedModule);
			return null;
		}).when(moduleHandler).collectPayload(anyList(), any(Map.class), anyList());
		// collectPayloadRelatedResources：解析出导出包里的连接（键 = 导出侧连接 id）
		org.mockito.Mockito.doAnswer(invocation -> {
			Map<ResourceType, Map<String, ?>> resourceMap = invocation.getArgument(1);
			((Map<String, Object>) resourceMap.computeIfAbsent(ResourceType.CONNECTION, k -> new java.util.HashMap<>()))
					.put(CONNECTION_ID, targetConnection);
			return null;
		}).when(moduleHandler).collectPayloadRelatedResources(any(Map.class), any(Map.class), any(Map.class),
				any(UserDetail.class));

		when(resourceHandlerRegistry.getHandler(ResourceType.MODULE)).thenReturn(moduleHandler);
		when(resourceHandlerRegistry.getAllHandlers()).thenReturn(Collections.singletonList(moduleHandler));
		// DB 里没有同 _id 的 Module → diff 判「新增」；连接同 _id 存在 → conMap 命中
		when(modulesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());
		when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(Collections.singletonList(targetConnection));
	}

	@Test
	@DisplayName("Module 落库之后才触发索引落地，且拿到的是真实 conMap（旧连接 id → 目标连接）")
	@SuppressWarnings("unchecked")
	void landsServingIndexesAfterModulesImported() {
		wireHandlers();

		groupInfoService.executeImportApisStandaloneAsync(payloads(), ImportModeEnum.REPLACE, user, new ObjectId());

		ArgumentCaptor<List<ModulesDto>> modulesCaptor = ArgumentCaptor.forClass(List.class);
		ArgumentCaptor<Map<String, DataSourceConnectionDto>> conMapCaptor = ArgumentCaptor.forClass(Map.class);
		InOrder order = inOrder(modulesService, servingIndexLandingService);
		order.verify(modulesService).batchImport(anyList(), eq(user), eq(ImportModeEnum.REPLACE), any(Map.class),
				isNull());
		order.verify(servingIndexLandingService).landAfterImport(modulesCaptor.capture(), conMapCaptor.capture(),
				eq(user));

		assertEquals(1, modulesCaptor.getValue().size());
		assertSame(importedModule, modulesCaptor.getValue().get(0), "落地要用刚导入的那批 Module 本体（声明就在它身上）");
		assertSame(targetConnection, conMapCaptor.getValue().get(CONNECTION_ID),
				"conMap 必须是导入现场那一张：目标连接解析错 = 索引建到别的库（ADR-0002）");
	}

	@Test
	@DisplayName("这批导入没有任何 Module 时不触发落地")
	void skipsLandingWhenNothingImported() {
		when(resourceHandlerRegistry.getHandler(ResourceType.MODULE)).thenReturn(null);

		groupInfoService.executeImportApisStandaloneAsync(
				Collections.<String, List<TaskUpAndLoadDto>>emptyMap(), ImportModeEnum.REPLACE, user, new ObjectId());

		verify(servingIndexLandingService, org.mockito.Mockito.never())
				.landAfterImport(anyList(), any(Map.class), any(UserDetail.class));
	}
}

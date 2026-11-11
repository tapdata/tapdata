package com.tapdata.tm.servingindex;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.handler.ModuleResourceHandler;
import com.tapdata.tm.group.repostitory.GroupInfoRepository;
import com.tapdata.tm.group.service.GroupInfoService;
import com.tapdata.tm.group.vo.FieldChange;
import com.tapdata.tm.group.vo.ResourceDiff;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.ServingIndex;
import com.tapdata.tm.module.dto.ServingIndexField;
import com.tapdata.tm.module.dto.ServingIndexNormalizer;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * TAP-12057 · P2 验收条件②：{@code servingIndexes} 必须<b>进导出包</b>、且「只改索引」必须<b>被 preview 认出</b>。
 *
 * <p>钉的是 CICD 真实链路的两半（CICD worker 的 {@code preview-resource.sh apis} 打的就是
 * {@code /api/groupInfo/preview/apis}）：</p>
 * <ol>
 *   <li><b>导出</b>：{@code ModuleResourceHandler.buildExportPayload}（整 DTO 序列化、清敏感字段）
 *       → {@code GroupInfoService.parseAndStripExportJson}（按集合剔运行时字段）
 *       → {@code buildExportContents} 落 {@code API/{id}_Module.json}。
 *       任一环把 {@code servingIndexes} 剔掉，索引就传不到下一环境。</li>
 *   <li><b>diff</b>：{@code buildApiDiff} 的 {@code MODULE_EXCLUDED_FIELDS} 若把 {@code servingIndexes}
 *       排除，「只改索引」就会被判成无变更、CICD 里悄悄跳过。</li>
 * </ol>
 *
 * <p>文件侧一律走真实导出路径构造（而非直接序列化 DTO），否则测不出 strip 环节的剔除。</p>
 */
@ExtendWith(MockitoExtension.class)
class ServingIndexesExportDiffTest {

	@Mock
	private ModulesService modulesService;

	private GroupInfoService groupInfoService;
	private ModuleResourceHandler moduleResourceHandler;
	private UserDetail user;

	@BeforeEach
	void setUp() {
		groupInfoService = new GroupInfoService(mock(GroupInfoRepository.class));
		ReflectionTestUtils.setField(groupInfoService, "modulesService", modulesService);
		moduleResourceHandler = new ModuleResourceHandler();
		user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
				"accessCode", false, false, false, false,
				Collections.singletonList(new SimpleGrantedAuthority("role")));
	}

	/** 一条复合索引：升序 + 降序各一段，方向必须原样穿过导出链路。 */
	private static ModulesDto moduleWith(ObjectId id, ServingIndex... indexes) {
		ModulesDto dto = new ModulesDto();
		dto.setId(id);
		dto.setName("policy_api");
		dto.setTableName("POLICY");
		dto.setCustomId("customId");
		dto.setLastUpdBy("lastUpdBy");
		dto.setLastUpdAt(new java.util.Date(1_700_000_000_000L));
		dto.setServingIndexes(ServingIndexNormalizer.normalize(new ArrayList<>(Arrays.asList(indexes))));
		return dto;
	}

	private static ServingIndex index(String name, String... fields) {
		List<ServingIndexField> fieldList = new ArrayList<>();
		for (int i = 0; i < fields.length; i++) {
			// 交替升/降序，保证方向差异也在覆盖内
			fieldList.add(new ServingIndexField(fields[i], i % 2 == 0));
		}
		ServingIndex idx = new ServingIndex(name, true, fieldList);
		idx.setCollected(Boolean.TRUE);
		return idx;
	}

	/** 按真实导出路径把一个 Module 变成导入侧看到的文件 JSON（含 strip 环节）。 */
	private String exportedJson(ModulesDto dto) {
		// maskSecrets=true 与本用例写就时的行为一致（那会儿导出无条件脱敏）；脱敏只作用于连接与
		// 任务自带的 metadata，不影响本用例断言的 Module 载荷（[ADR-0034]）。
		List<TaskUpAndLoadDto> payload = moduleResourceHandler.buildExportPayload(
				new ArrayList<>(Collections.singletonList(dto)), user, true);
		assertEquals(1, payload.size());
		Object stripped = ReflectionTestUtils.invokeMethod(groupInfoService, "parseAndStripExportJson",
				GroupConstants.COLLECTION_MODULES, payload.get(0).getJson());
		return JsonUtil.toJsonUseJackson(stripped);
	}

	private Map<String, List<TaskUpAndLoadDto>> payloadsOf(String fileJson) {
		Map<String, List<TaskUpAndLoadDto>> payloads = new LinkedHashMap<>();
		payloads.put("Module.json", new ArrayList<>(Collections.singletonList(
				new TaskUpAndLoadDto(GroupConstants.COLLECTION_MODULES, fileJson))));
		return payloads;
	}

	private static List<String> changedFieldPaths(ResourceDiff diff) {
		assertEquals(1, diff.getUpdate().size(), "同 _id 的 Module 应落在 update，而非 add");
		List<FieldChange> changes = diff.getUpdate().get(0).getChanges();
		assertNotNull(changes, "update 项必须带字段级变更");
		return changes.stream().map(FieldChange::getField).toList();
	}

	@Test
	@DisplayName("导出包 API/{id}_Module.json 必须含 servingIndexes（含方向），且能原样解析回 DTO")
	void exportPackageCarriesServingIndexes() {
		ObjectId id = new ObjectId();
		ModulesDto dto = moduleWith(id, index("ix_policy", "POLICY_ID", "TS"));

		// ① handler 产出的 payload JSON
		String fileJson = exportedJson(dto);
		assertTrue(fileJson.contains("\"servingIndexes\""),
				"导出 JSON 缺 servingIndexes，索引传不到下一环境：" + fileJson);
		assertFalse(fileJson.contains("\"last_updated\""), "运行时字段应被 strip 掉");

		// ② 导入侧按 ModulesDto 解析回来：名字/字段顺序/方向都不能丢
		ModulesDto parsed = JsonUtil.parseJsonUseJackson(fileJson, ModulesDto.class);
		assertNotNull(parsed.getServingIndexes());
		assertEquals(1, parsed.getServingIndexes().size());
		ServingIndex parsedIndex = parsed.getServingIndexes().get(0);
		assertEquals("ix_policy", parsedIndex.getName());
		assertEquals(Boolean.TRUE, parsedIndex.getUnique());
		assertEquals(Arrays.asList("POLICY_ID", "TS"),
				parsedIndex.getFields().stream().map(ServingIndexField::getField).toList());
		assertEquals(Arrays.asList(Boolean.TRUE, Boolean.FALSE),
				parsedIndex.getFields().stream().map(ServingIndexField::getAsc).toList());

		// ③ 落盘文件名与内容
		Map<String, List<TaskUpAndLoadDto>> payloadsByType = new LinkedHashMap<>();
		payloadsByType.put("MODULE", new ArrayList<>(Collections.singletonList(
				new TaskUpAndLoadDto(GroupConstants.COLLECTION_MODULES, JsonUtil.toJsonUseJackson(dto)))));
		// 第 4 个参数 maskSecrets 是 [ADR-0034] 的导出脱敏分流带来的。本用例只关心 Module 载荷里
		// 有没有 servingIndexes（脱敏只作用于连接），传 true 是为了与本用例写就时的行为一致 ——
		// 那会儿导出无条件脱敏。⚠ 这里是反射调用：签名再变一次，编译期照样不报错，只会在运行期
		// 抛 IllegalStateException: Method not found。
		@SuppressWarnings("unchecked")
		Map<String, byte[]> contents = (Map<String, byte[]>) ReflectionTestUtils.invokeMethod(
				groupInfoService, "buildExportContents",
				new ArrayList<TaskUpAndLoadDto>(), payloadsByType, new LinkedHashMap<String, byte[]>(), true);
		assertNotNull(contents);
		String entry = "API/" + id.toHexString() + "_Module.json";
		assertTrue(contents.containsKey(entry), "导出包应有 " + entry + "，实际：" + contents.keySet());
		assertTrue(new String(contents.get(entry), StandardCharsets.UTF_8).contains("servingIndexes"),
				entry + " 内容缺 servingIndexes");
	}

	@Test
	@DisplayName("只改索引（新增一条）：preview 落 update，且 changedFields 指向 servingIndexes")
	void indexOnlyChangeShowsUpInPreview() {
		ObjectId id = new ObjectId();
		ModulesDto inDb = moduleWith(id, index("ix_policy", "POLICY_ID", "TS"));
		ModulesDto inFile = moduleWith(id,
				index("ix_policy", "POLICY_ID", "TS"),
				index("ix_region", "REGION"));
		when(modulesService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(new ArrayList<>(Collections.singletonList(inDb)));

		ResourceDiff diff = ReflectionTestUtils.invokeMethod(
				groupInfoService, "buildApiDiff", payloadsOf(exportedJson(inFile)), user);

		assertNotNull(diff);
		List<String> paths = changedFieldPaths(diff);
		assertTrue(paths.stream().anyMatch(p -> p.startsWith("servingIndexes")),
				"「只改索引」未被 preview 认出，changedFields=" + paths);
	}

	@Test
	@DisplayName("只改索引方向（升→降）：同样必须被 preview 认出——方向是索引身份的一部分")
	void directionOnlyChangeShowsUpInPreview() {
		ObjectId id = new ObjectId();
		ModulesDto inDb = moduleWith(id, index("ix_policy", "POLICY_ID"));
		ModulesDto inFile = moduleWith(id, new ServingIndex("ix_policy", true,
				new ArrayList<>(Collections.singletonList(new ServingIndexField("POLICY_ID", false)))));
		when(modulesService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(new ArrayList<>(Collections.singletonList(inDb)));

		ResourceDiff diff = ReflectionTestUtils.invokeMethod(
				groupInfoService, "buildApiDiff", payloadsOf(exportedJson(inFile)), user);

		assertNotNull(diff);
		List<String> paths = changedFieldPaths(diff);
		assertTrue(paths.stream().anyMatch(p -> p.startsWith("servingIndexes") && p.endsWith("asc")),
				"索引方向变更未被 preview 认出，changedFields=" + paths);
	}

	@Test
	@DisplayName("索引未变：整个 update 必须为空 —— 一条噪声都不许有（[ADR-0037] 收窄后抬硬）")
	void unchangedIndexesProduceNoServingIndexChange() {
		// 抬硬历史：本用例原先写成 `diff.getUpdate().isEmpty() ? emptyList : ...`，只断言「没有
		// servingIndexes 前缀的变更」。那份容忍不是设计，是当时 update 里还躺着 last_updated /
		// customId 这类管道噪声（module-diff-scope 的 bug 本身）。收窄之后噪声没有了，
		// 断言随之抬到「整个 update 为空」——这一条同时守着索引与噪声两件事。
		ObjectId id = new ObjectId();
		ModulesDto inDb = moduleWith(id, index("ix_policy", "POLICY_ID", "TS"));
		ModulesDto inFile = moduleWith(id, index("ix_policy", "POLICY_ID", "TS"));
		when(modulesService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(new ArrayList<>(Collections.singletonList(inDb)));

		ResourceDiff diff = ReflectionTestUtils.invokeMethod(
				groupInfoService, "buildApiDiff", payloadsOf(exportedJson(inFile)), user);

		assertNotNull(diff);
		assertTrue(diff.getUpdate().isEmpty(), () -> {
			List<FieldChange> changes = diff.getUpdate().get(0).getChanges();
			return "索引与内容都没变，却报了变更：" + (changes == null
					? "changes=null" : changes.stream().map(FieldChange::getField).toList());
		});
	}
}

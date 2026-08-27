package com.tapdata.tm.group.service;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.handler.ModuleResourceHandler;
import com.tapdata.tm.group.repostitory.GroupInfoRepository;
import com.tapdata.tm.group.vo.FieldChange;
import com.tapdata.tm.group.vo.ResourceDiff;
import com.tapdata.tm.group.vo.ResourceDiffItem;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.Param;
import com.tapdata.tm.module.dto.PathSetting;
import com.tapdata.tm.module.dto.ServingIndex;
import com.tapdata.tm.module.dto.ServingIndexField;
import com.tapdata.tm.module.dto.ServingIndexNormalizer;
import com.tapdata.tm.module.dto.Sort;
import com.tapdata.tm.module.dto.Where;
import com.tapdata.tm.module.entity.ApiAlarmConfig;
import com.tapdata.tm.module.entity.Path;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * module-diff-scope · T1 实测：把「未改动的 API 为什么每次都被判成变更」从读码结论变成实测事实。
 *
 * <p>两个实验，收窄前都会红，<b>失败信息里那份清单就是实测产出</b>（回填
 * {@code research/2026-08-module-diff-noise.md}）：</p>
 * <ol>
 *   <li>{@link #exportPipelineAloneProducesNoUpdate()} —— 两侧内容逐字相同、只有导出管道走过一遍。
 *       报出来的每一条都是<b>管道自身的不对称</b>（strip / 就地置空 / 排序），与目标环境无关。</li>
 *   <li>{@link #unchangedModuleAcrossEnvironmentsProducesNoUpdate()} —— DB 侧按「目标环境风格」另造实例
 *       （时间戳、运行统计、发布状态、账号、字段 id 与类型细节都不同），<b>用户可编辑内容逐字相同</b>。
 *       这就是 T4 用例 1，收窄后必须绿。</li>
 * </ol>
 *
 * <p>⚠ 两侧必须是<b>两个独立实例</b>：{@code ModuleResourceHandler.buildExportPayload} 就地改传进去的
 * DTO（{@code setCustomId(null)} / {@code setLastUpdBy(null)} / {@code setStatus(null)}，外加对 fields 排序），
 * 复用同一引用会把 DB 侧这几项一起抹平 —— 用例测不到它要测的不对称、却照样绿。</p>
 */
@ExtendWith(MockitoExtension.class)
class GroupInfoServiceModuleDiffScopeTest {

	/** 播种口径：同一份 API 在源环境与目标环境各自的样子。用户可编辑内容两者逐字相同。 */
	private enum Env { SOURCE, TARGET }

	/** 两侧共享：连接 _id 跨环境保留（conMap 按 _id 查目标库，见 buildConMapFromPayload）。 */
	private static final String CONNECTION_ID = "660000000000000000000001";

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

	// ------------------------------------------------------------------ 实验

	@Test
	@DisplayName("T1-A 导出管道自身的不对称：两侧内容逐字相同时 diff 必须为空（收窄前会红）")
	void exportPipelineAloneProducesNoUpdate() {
		ObjectId id = new ObjectId();
		ResourceDiff diff = diffOf(fullModule(id, Env.SOURCE), fullModule(id, Env.SOURCE));

		assertEquals(0, diff.getAdd().size(), "同 _id 的 Module 应落 update 而不是 add：" + report(diff));
		assertTrue(diff.getUpdate().isEmpty(),
				() -> "两侧内容逐字相同却报了变更 —— 下列每一条都是导出管道自身的不对称：\n" + report(diff));
	}

	@Test
	@DisplayName("T1-B 未改动的 API 跨环境：目标环境派生字段不同，diff 仍必须为空（收窄前会红）")
	void unchangedModuleAcrossEnvironmentsProducesNoUpdate() {
		ObjectId id = new ObjectId();
		ResourceDiff diff = diffOf(fullModule(id, Env.SOURCE), fullModule(id, Env.TARGET));

		assertEquals(0, diff.getAdd().size(), "同 _id 的 Module 应落 update 而不是 add：" + report(diff));
		assertTrue(diff.getUpdate().isEmpty(),
				() -> "没人改过这个 API，却被判成有变更 —— 下列每一条都是「未改动也报变更」的实测噪声：\n"
						+ report(diff));
	}

	@Test
	@DisplayName("T1-C MODULE 导出载荷不带 MetadataInstances —— 第三个消费者今天推的本来就是空列表")
	void moduleExportPayloadCarriesNoMetadataInstances() {
		// executeImportApisStandaloneAsync:1205-1208 的 refreshMetadataLastUpdate + metadataInstancesService
		// .batchImport 传的是 metadataByType.get(MODULE)，而那份列表只由 ModuleResourceHandler.collectPayload
		// 从 Module.json 桶里的 MetadataInstances 条目填充。导出侧根本不往那个桶里放这种条目 ⇒ 恒空。
		List<TaskUpAndLoadDto> payload = moduleResourceHandler.buildExportPayload(
				new ArrayList<>(Collections.singletonList(fullModule(new ObjectId(), Env.SOURCE))), user, true);

		Map<String, ModulesDto> resourceMap = new LinkedHashMap<>();
		List<MetadataInstancesDto> metadata = new ArrayList<>();
		moduleResourceHandler.collectPayload(payload, resourceMap, metadata);

		assertEquals(1, resourceMap.size(), "Module 本体必须收得到");
		assertTrue(metadata.isEmpty(),
				"MODULE 载荷里出现了 MetadataInstances —— 那两句从此不再是空转，"
						+ "「零变更 ⇒ 整块不跑」会真的停掉一次推送，ADR-0037 第 ⑦ 项要重判");
	}

	// -------------------------------------------------------------- 链路脚手架

	/** 文件侧走真实导出路径（buildExportPayload → parseAndStripExportJson），DB 侧走 findAllDto。 */
	private ResourceDiff diffOf(ModulesDto fileSide, ModulesDto dbSide) {
		when(modulesService.findAllDto(any(Query.class), any(UserDetail.class)))
				.thenReturn(new ArrayList<>(Collections.singletonList(dbSide)));
		ResourceDiff diff = ReflectionTestUtils.invokeMethod(
				groupInfoService, "buildApiDiff", payloadsOf(exportedJson(fileSide)), user);
		assertNotNull(diff);
		return diff;
	}

	private String exportedJson(ModulesDto dto) {
		List<TaskUpAndLoadDto> payload = moduleResourceHandler.buildExportPayload(
				new ArrayList<>(Collections.singletonList(dto)), user, true);
		assertEquals(1, payload.size());
		Object stripped = ReflectionTestUtils.invokeMethod(groupInfoService, "parseAndStripExportJson",
				GroupConstants.COLLECTION_MODULES, payload.get(0).getJson());
		return JsonUtil.toJsonUseJackson(stripped);
	}

	private static Map<String, List<TaskUpAndLoadDto>> payloadsOf(String fileJson) {
		Map<String, List<TaskUpAndLoadDto>> payloads = new LinkedHashMap<>();
		payloads.put("Module.json", new ArrayList<>(Collections.singletonList(
				new TaskUpAndLoadDto(GroupConstants.COLLECTION_MODULES, fileJson))));
		return payloads;
	}

	/** 实测清单本体：路径 + 两侧的值，直接抄进 research 笔记。 */
	private static String report(ResourceDiff diff) {
		StringBuilder sb = new StringBuilder();
		sb.append("add=").append(diff.getAdd().size())
				.append(" update=").append(diff.getUpdate().size()).append('\n');
		for (ResourceDiffItem item : diff.getUpdate()) {
			List<FieldChange> changes = item.getChanges() == null
					? Collections.emptyList() : item.getChanges();
			sb.append("update[").append(item.getName()).append("] changes=")
					.append(changes.size()).append('\n');
			for (FieldChange c : changes) {
				sb.append("  - ").append(c.getField())
						.append("\n      db   = ").append(abbrev(c.getFrom()))
						.append("\n      file = ").append(abbrev(c.getTo())).append('\n');
			}
		}
		return sb.toString();
	}

	private static String abbrev(Object v) {
		String s = String.valueOf(v);
		return s.length() <= 200 ? s : s.substring(0, 200) + "…(" + s.length() + " chars)";
	}

	// ------------------------------------------------------------------ 播种

	/**
	 * 一个字段齐全的 Module。<b>用户可编辑内容两个 Env 逐字相同</b>；不同的只有环境派生 / 运行时产物，
	 * 每一处都在行内注明它凭什么会不同。
	 */
	private static ModulesDto fullModule(ObjectId id, Env env) {
		boolean src = env == Env.SOURCE;
		ModulesDto m = new ModulesDto();

		// —— BaseDto ——
		m.setId(id);
		m.setCustomId(src ? "customer-sit" : "customer-uat");                 // 租户 id，导出置 null
		m.setCreateAt(new Date(1_700_000_000_000L));
		m.setLastUpdAt(new Date(src ? 1_700_000_111_000L : 1_800_000_222_000L)); // 导入必刷（refreshModuleLastUpdate）
		m.setUserId(src ? "user-sit" : "user-uat");
		m.setLastUpdBy(src ? "alice" : "bob");
		m.setCreateUser(src ? "alice" : "bob");
		m.setPermissionActions(new LinkedHashSet<>(
				Collections.singletonList(src ? "Data_SIT" : "Data_UAT")));

		// —— 用户可编辑：两侧必须逐字相同 ——
		m.setName("policy_api");
		m.setTableName("POLICY");
		m.setApiVersion("v1");
		m.setBasePath("api");
		m.setPrefix("insurance");
		m.setPath("/api/insurance/v1/policy");
		m.setApiType("defaultApi");
		m.setOperationType("GET");
		m.setPathAccessMethod("default");
		m.setLimit(500);
		m.setDescription("保单查询 API");
		m.setDescribtion("legacy description");
		m.setProject("insurance");
		m.setCreateType("manual");
		m.setReadPreference("primary");
		m.setReadPreferenceTag("");
		m.setReadConcern("local");
		m.setListtags(new ArrayList<>(Collections.singletonList(
				new Tag("650000000000000000000009", "保险"))));
		m.setPathSetting(new ArrayList<>(PathSetting.DEFAULT_PATH_SETTING));
		ApiAlarmConfig alarm = new ApiAlarmConfig();
		alarm.setEmailReceivers(new ArrayList<>(Collections.singletonList("ops@example.com")));
		m.setApiAlarmConfig(alarm);
		ServingIndex ix = new ServingIndex("ix_policy", true, new ArrayList<>(Arrays.asList(
				new ServingIndexField("CUSTOMER_NO", true), new ServingIndexField("TS", false))));
		ix.setCollected(Boolean.TRUE);
		m.setServingIndexes(ServingIndexNormalizer.normalize(
				new ArrayList<>(Collections.singletonList(ix))));
		m.setPaths(new ArrayList<>(Collections.singletonList(apiPath(env))));
		m.setFields(new ArrayList<>(Arrays.asList(
				field("POLICY_ID", "保单号", env, 0),
				field("CUSTOMER_NO", "客户号", env, 1),
				field("TS", null, env, 2))));

		// —— 连接：_id 跨环境保留（import 的 conMap 按 _id 查目标库），名字由目标连接覆写 ——
		m.setDataSource(CONNECTION_ID);
		m.setConnectionId(CONNECTION_ID);
		m.setConnection(new ObjectId(CONNECTION_ID));
		m.setConnectionType("postgres");
		m.setConnectionName(src ? "PG_SIT" : "PG_UAT");   // updateConnectionIds 取目标连接的名字

		// —— 环境派生 / 运行时 ——
		m.setStatus(src ? "pending" : "active");           // 目标已发布
		m.setPublishStatus(src ? "unpublished" : "published");
		m.setLast_updated(new Date(src ? 1_700_000_111_000L : 1_800_000_222_000L));
		m.setUser(src ? "sit-owner" : "uat-owner");
		m.setAccess_token(src ? "token-sit" : "token-uat");
		m.setEmail(src ? "sit@example.com" : "uat@example.com");
		m.setIsDeleted(Boolean.FALSE);
		m.setVisitCount(src ? 0L : 12_345L);               // 目标环境有流量
		m.setLatency(src ? 0L : 37L);
		m.setResponseTime(src ? 0L : 41L);
		m.setReqBytes(src ? 0L : 8_192L);
		m.setResRows(src ? 0L : 990L);
		m.setFailRate(src ? 0 : 2);
		return m;
	}

	private static Path apiPath(Env env) {
		Path p = new Path();
		p.setName("customerQuery");
		p.setPath("/policy");
		p.setMethod("POST");
		p.setType("default");
		p.setResult("");
		p.setCreateType("manual");
		p.setDescription("按客户号查保单");
		p.setAcl(new ArrayList<>(Collections.singletonList("admin")));
		p.setFullCustomQuery(Boolean.FALSE);
		p.setCustomWhere("");
		Param param = new Param();
		param.setName("customerNo");
		param.setType("string");
		param.setDefaultvalue("");
		param.setDescription("客户号");
		param.setRequired(true);
		p.setParams(new ArrayList<>(Collections.singletonList(param)));
		Where where = new Where();
		where.setFieldName("CUSTOMER_NO");
		where.setParameter("customerNo");
		where.setOperator("eq");
		where.setCondition("and");
		p.setWhere(new ArrayList<>(Collections.singletonList(where)));
		Sort sort = new Sort();
		sort.setFieldName("TS");
		sort.setType("DESC");
		p.setSort(new ArrayList<>(Collections.singletonList(sort)));
		// 暴露集合（真值在 paths[0].fields）
		p.setFields(new ArrayList<>(Arrays.asList(
				field("POLICY_ID", "保单号", env, 0),
				field("CUSTOMER_NO", "客户号", env, 1),
				field("TS", null, env, 2))));
		p.setAvailableQueryField(new ArrayList<>(Collections.singletonList(
				field("CUSTOMER_NO", "客户号", env, 1))));
		p.setRequiredQueryField(new ArrayList<>(Collections.singletonList(
				field("CUSTOMER_NO", "客户号", env, 1))));
		return p;
	}

	/**
	 * 一个属性铺开的 Field。{@code field_name} 与 {@code field_alias} 两侧相同（那是用户编辑的东西），
	 * 其余全是目标环境重新推演模型后会拿到自己一套的产物。
	 */
	private static Field field(String name, String alias, Env env, int position) {
		boolean src = env == Env.SOURCE;
		Field f = new Field();
		// 用户可编辑
		f.setFieldName(name);
		f.setFieldAlias(alias);                                   // TS 故意无别名：覆盖「空别名不互报差异」
		// 环境派生 / 模型推演
		f.setId(src ? "sit-fid-" + name : "uat-fid-" + name);
		f.setDataType(src ? "varchar(64)" : "character varying(64)");   // data_type  (String)
		f.setDataType1(src ? 12 : 1);                                   // dataType   (Integer，另一个 key)
		f.setPureDataType(src ? "varchar" : "character varying");
		f.setPrecision(src ? 64 : 128);                                 // data_precision
		f.setScale(src ? 0 : 2);                                        // data_scale
		f.setLength(src ? 64 : 128);                                    // data_length
		f.setColumnSize(src ? 64 : 128);
		f.setColumnPosition(src ? position : position + 10);
		f.setTapType(src ? "{\"type\":10}" : "{\"type\":11}");
		f.setOriginalDataType(src ? "VARCHAR" : "TEXT");
		f.setJavaType(src ? "String" : "java.lang.String");             // java_type
		f.setJavaType1(src ? "String" : "java.lang.String");            // javaType
		f.setOriginalJavaType(src ? "String" : "java.lang.String");
		f.setOriPrecision(src ? 64 : 128);
		f.setDataCode(src ? 1 : 2);
		f.setNodeDataType(src ? "varchar" : "text");
		f.setSource(src ? Field.SOURCE_AUTO : Field.SOURCE_JOB_ANALYZE);
		// 两侧一致的其余属性
		f.setTableName("POLICY");
		f.setOriginalFieldName(name);
		f.setPrimaryKey(position == 0);
		f.setPrimaryKeyPosition(position == 0 ? 1 : 0);
		f.setIsNullable(position != 0);
		f.setVisible(Boolean.TRUE);
		f.setComment(name + " 注释");
		f.setKey(name);
		return f;
	}
}

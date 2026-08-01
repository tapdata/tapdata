package com.tapdata.tm.servingindex;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.ServingIndex;
import com.tapdata.tm.module.dto.ServingIndexField;
import com.tapdata.tm.module.dto.ServingIndexLandingWorkList;
import com.tapdata.tm.module.dto.UnresolvedServingIndexTarget;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3-1 · TM 落地编排服务（{@link ServingIndexLandingService}）。
 *
 * <p>导入落完连接与 Module 之后，本服务把「这批 Module 的索引声明」翻译成 {@code (目标连接, 集合)} 工作项
 * ——目标连接只从 conMap 拿（<b>ADR-0002</b>）。聚合口径的用例在 {@code ServingIndexLandingTargetsTest}；
 * 这里钉的是服务层契约：<b>不吞</b>落不了地的记录、空入参不炸。</p>
 */
class ServingIndexLandingServiceTest {

	private static final String CONNECTION_ID = "64b7e1f4c9e77a0001aa0001";

	private ServingIndexLandingService service;
	private UserDetail user;

	@BeforeEach
	void setUp() {
		service = new ServingIndexLandingService();
		user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
				"accessCode", false, false, false, false,
				Collections.singletonList(new SimpleGrantedAuthority("role")));
	}

	private static ModulesDto api(String name, String connectionId, String tableName, ServingIndex... indexes) {
		ModulesDto module = new ModulesDto();
		module.setId(new ObjectId());
		module.setName(name);
		module.setConnectionId(connectionId);
		module.setTableName(tableName);
		module.setServingIndexes(new ArrayList<>(Arrays.asList(indexes)));
		return module;
	}

	private static ServingIndex idx(String name, String field, Boolean asc) {
		return new ServingIndex(name, null,
				new ArrayList<>(Collections.singletonList(new ServingIndexField(field, asc))));
	}

	private static Map<String, DataSourceConnectionDto> conMap() {
		DataSourceConnectionDto target = new DataSourceConnectionDto();
		target.setId(new ObjectId(CONNECTION_ID));
		target.setName("MDM-生产库");
		Map<String, DataSourceConnectionDto> map = new LinkedHashMap<>();
		map.put(CONNECTION_ID, target);
		return map;
	}

	@Test
	@DisplayName("产出 (连接,集合) 工作项，连接取自 conMap")
	void buildsWorkListFromImportedModules() {
		ServingIndexLandingWorkList work = service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true))),
				conMap(), user);

		assertEquals(1, work.getTargets().size());
		assertEquals(CONNECTION_ID, work.getTargets().get(0).getConnectionId());
		assertEquals("CUSTOMER", work.getTargets().get(0).getTableName());
		assertEquals(1, work.declaredCount());
		assertTrue(work.getUnresolved().isEmpty());
	}

	@Test
	@DisplayName("conMap 未命中的声明进 unresolved 桶——不静默跳过（否则等于「部署报成功、索引没建」）")
	void carriesUnresolvedInsteadOfDroppingThem() {
		ServingIndexLandingWorkList work = service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true))),
				new HashMap<>(), user);

		assertTrue(work.getTargets().isEmpty());
		assertEquals(1, work.getUnresolved().size());
		assertEquals(UnresolvedServingIndexTarget.Reason.CONNECTION_UNRESOLVED,
				work.getUnresolved().get(0).getReason());
	}

	@Test
	@DisplayName("没有 Module / 没有 conMap / 没有声明都不炸，返回空工作表")
	void toleratesEmptyInput() {
		assertTrue(service.landAfterImport(null, null, user).isEmpty());
		assertTrue(service.landAfterImport(Collections.<ModulesDto>emptyList(), conMap(), user).isEmpty());
		assertTrue(service.landAfterImport(
				Collections.singletonList(api("查客户", CONNECTION_ID, "CUSTOMER")), conMap(), user).isEmpty());
	}

	@Test
	@DisplayName("多 API 共表 → 一个工作项，声明并集（去重在 planner，不在本层）")
	void unionsSameTableAcrossApis() {
		List<ModulesDto> modules = Arrays.asList(
				api("查客户", CONNECTION_ID, "CUSTOMER", idx("a_1", "a", true)),
				api("查客户明细", CONNECTION_ID, "CUSTOMER", idx("b_-1", "b", false)));

		ServingIndexLandingWorkList work = service.landAfterImport(modules, conMap(), user);

		assertEquals(1, work.getTargets().size());
		assertEquals(2, work.declaredCount());
		assertEquals(Arrays.asList("查客户", "查客户明细"), work.getTargets().get(0).getSourceApis());
	}
}

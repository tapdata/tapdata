package com.tapdata.tm.module.dto;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P4-1 · <b>首次部署</b>时的计划来源切分（纯函数）。TAP-12057。
 *
 * <p>索引腿的 preview 排在部署矩阵之前，而目标环境的连接是由排在它后面的 {@code connections} 腿建的。
 * 于是「首次部署到全新环境」必然出现一种中间态：<b>包里带了这个连接、目标环境还没有</b>。
 * 此前这种情形被当成 {@code CONNECTION_UNRESOLVED} 报红，整条流水线在部署任何东西之前就自锁。</p>
 *
 * <p>切分口径：连接已落地 → 交给既有 dry-run 走<b>真实比对</b>；连接待落地 → 按<b>包内声明</b>出计划；
 * 包里也没带 → 仍旧交给 dry-run，由既有逻辑报 {@code CONNECTION_UNRESOLVED}（那种确实补不回来）。</p>
 */
class ServingIndexPendingPlansTest {

	private static final String CONNECTION_ID = "64b7e1f4c9e77a0001aa0001";

	private static ServingIndexField field(String name, boolean asc) {
		return new ServingIndexField(name, asc);
	}

	private static ServingIndex index(String name, Boolean unique, ServingIndexField... fields) {
		return new ServingIndex(name, unique, new ArrayList<>(Arrays.asList(fields)));
	}

	private static ModulesDto module(String name, String connectionId, String table, ServingIndex... indexes) {
		ModulesDto module = new ModulesDto();
		module.setId(new ObjectId());
		module.setName(name);
		module.setConnectionId(connectionId);
		module.setTableName(table);
		module.setServingIndexes(new ArrayList<>(Arrays.asList(indexes)));
		return module;
	}

	private static DataSourceConnectionDto connection(String name) {
		DataSourceConnectionDto connection = new DataSourceConnectionDto();
		connection.setId(new ObjectId(CONNECTION_ID));
		connection.setName(name);
		return connection;
	}

	private static Map<String, DataSourceConnectionDto> map(String id, DataSourceConnectionDto connection) {
		Map<String, DataSourceConnectionDto> out = new HashMap<>();
		out.put(id, connection);
		return out;
	}

	@Test
	@DisplayName("目标环境还没有这个连接、包里带了 → 按声明出计划行，不交给 dry-run")
	void modulesWhoseConnectionHasNotLandedYetBecomePlannedRows() {
		ModulesDto module = module("customer_by_country", CONNECTION_ID, "MDM_CUSTOMER",
				index("LAST_CHANGE_-1", false, field("LAST_CHANGE", false)));

		ServingIndexPendingPlans.Split split = ServingIndexPendingPlans.split(
				Collections.singletonList(module), Collections.emptyMap(), map(CONNECTION_ID, connection("mdm")));

		assertTrue(split.getResolved().isEmpty(), "连接没落地就读不了目标库，不能交给 dry-run");
		assertEquals(1, split.getPendingRows().size());
		ServingIndexPlanRow row = split.getPendingRows().get(0);
		assertEquals("mdm", row.getConnection(), "取包里那个连接的名字——落地后同名同 id 就是它");
		assertEquals("MDM_CUSTOMER", row.getTable());
		assertEquals("LAST_CHANGE_-1", row.getName());
		assertEquals("LAST_CHANGE:-1", row.getKeys(), "方向必须一眼可见（P0 那个缺陷正在这里）");
		assertEquals("customer_by_country", row.getDeclaredBy());
		assertEquals(ServingIndexPlanRow.BASIS_DECLARED, row.getBasis(),
				"审阅计划的人得知道这一行是「按声明」而非「与目标库比对」得出的");
	}

	@Test
	@DisplayName("目标环境已有该连接 → 交给既有 dry-run 走真实比对")
	void modulesWithLandedConnectionGoToDryRun() {
		ModulesDto module = module("customer_by_country", CONNECTION_ID, "MDM_CUSTOMER",
				index("CITY_1", false, field("CITY", true)));

		ServingIndexPendingPlans.Split split = ServingIndexPendingPlans.split(
				Collections.singletonList(module), map(CONNECTION_ID, connection("mdm")),
				map(CONNECTION_ID, connection("mdm")));

		assertEquals(1, split.getResolved().size());
		assertSame(module, split.getResolved().get(0));
		assertTrue(split.getPendingRows().isEmpty());
	}

	@Test
	@DisplayName("包里也没带这个连接 → 仍交给 dry-run，由既有逻辑报 CONNECTION_UNRESOLVED")
	void modulesWithoutPackagedConnectionStayWithDryRun() {
		ModulesDto module = module("customer_by_country", CONNECTION_ID, "MDM_CUSTOMER",
				index("CITY_1", false, field("CITY", true)));

		ServingIndexPendingPlans.Split split = ServingIndexPendingPlans.split(
				Collections.singletonList(module), Collections.emptyMap(), Collections.emptyMap());

		assertEquals(1, split.getResolved().size(), "这种落不了地是真的，必须让既有逻辑照常报红");
		assertTrue(split.getPendingRows().isEmpty());
	}

	@Test
	@DisplayName("没有勾选任何声明的 Module 不进计划表")
	void modulesWithoutDeclarationsProduceNoRows() {
		ServingIndex unchecked = index("ZIP_1", false, field("ZIP", true));
		unchecked.setCollected(false);
		ModulesDto module = module("customer_by_email", CONNECTION_ID, "MDM_CUSTOMER", unchecked);

		ServingIndexPendingPlans.Split split = ServingIndexPendingPlans.split(
				Collections.singletonList(module), Collections.emptyMap(), map(CONNECTION_ID, connection("mdm")));

		assertTrue(split.getPendingRows().isEmpty(), "未勾选的声明不能去建（ADR-0011 D2）");
		assertTrue(split.getResolved().isEmpty(), "无声明 = 无事可做，也不该拖累 dry-run");
	}

	@Test
	@DisplayName("一个 Module 的多条声明各出一行，来源 API 都指向它")
	void everyDeclarationGetsItsOwnRow() {
		ModulesDto module = module("customer_by_country", CONNECTION_ID, "MDM_CUSTOMER",
				index("LAST_CHANGE_-1", false, field("LAST_CHANGE", false)),
				index("COUNTRY_CODE_1_LAST_CHANGE_-1", false, field("COUNTRY_CODE", true), field("LAST_CHANGE", false)),
				index("EMAIL_1_CUSTOMER_ID_1", true, field("EMAIL", true), field("CUSTOMER_ID", true)));

		ServingIndexPendingPlans.Split split = ServingIndexPendingPlans.split(
				Collections.singletonList(module), Collections.emptyMap(), map(CONNECTION_ID, connection("mdm")));

		List<ServingIndexPlanRow> rows = split.getPendingRows();
		assertEquals(3, rows.size());
		assertEquals("COUNTRY_CODE:1,LAST_CHANGE:-1", rows.get(1).getKeys(),
				"与身份签名同一种渲染（ServingIndexSignature.ofFields），不能自成一格");
		assertTrue(rows.get(2).isUnique(), "unique 是创建时的参数，计划表得带上");
	}
}

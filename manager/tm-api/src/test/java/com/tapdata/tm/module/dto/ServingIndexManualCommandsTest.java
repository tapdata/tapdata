package com.tapdata.tm.module.dto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 部署计划表下方给人手工执行的 MongoDB 建索引语句（纯函数）。TAP-12057 · P4-1。
 *
 * <p>审批人看到「将创建这几条索引」之后，常常需要自己去目标库确认或先手工建一遍（大集合上更是如此）。
 * 语句由 TM 生成而不是让人照着表格手拼——手拼最容易拼错的正是<b>方向</b>，而「声明降序、建成升序」
 * 就是本工单起因的那个缺陷。</p>
 *
 * <p>与前端「推荐索引」的语句形态保持一致（{@code recommend.ts}）：一律带 {@code background: true}
 * ——语句是给人在**版本未知**的目标库上手工执行的，4.2+ 忽略它、4.0 及更早需要它。</p>
 *
 * <p><b>字段名一律加引号</b>：{@code POLICY.POLICY_STATUS} 这种点号路径不加引号就是非法 JS。</p>
 */
class ServingIndexManualCommandsTest {

	private static ServingIndexField field(String name, boolean asc) {
		return new ServingIndexField(name, asc);
	}

	private static ServingIndex index(String name, Boolean unique, ServingIndexField... fields) {
		return new ServingIndex(name, unique, new ArrayList<>(Arrays.asList(fields)));
	}

	@Test
	@DisplayName("单字段升序")
	void singleAscendingField() {
		assertEquals("db.MDM_CUSTOMER.createIndex({ \"CITY\": 1 }, { name: \"CITY_1\", background: true })",
				ServingIndexManualCommands.mongo("MDM_CUSTOMER", index("CITY_1", false, field("CITY", true))));
	}

	@Test
	@DisplayName("降序渲染成 -1——手工执行时方向拼错正是本工单起因的那个缺陷")
	void descendingRendersAsMinusOne() {
		assertEquals("db.MDM_CUSTOMER.createIndex({ \"LAST_CHANGE\": -1 }, "
						+ "{ name: \"LAST_CHANGE_-1\", background: true })",
				ServingIndexManualCommands.mongo("MDM_CUSTOMER",
						index("LAST_CHANGE_-1", false, field("LAST_CHANGE", false))));
	}

	@Test
	@DisplayName("复合索引按声明顺序拼，顺序错了就是另一条索引")
	void compositeKeepsDeclaredOrder() {
		assertEquals("db.MDM_CUSTOMER.createIndex({ \"COUNTRY_CODE\": 1, \"LAST_CHANGE\": -1 }, "
						+ "{ name: \"COUNTRY_CODE_1_LAST_CHANGE_-1\", background: true })",
				ServingIndexManualCommands.mongo("MDM_CUSTOMER",
						index("COUNTRY_CODE_1_LAST_CHANGE_-1", false,
								field("COUNTRY_CODE", true), field("LAST_CHANGE", false))));
	}

	@Test
	@DisplayName("unique 为真时带上；为假时不写（默认即非唯一）")
	void uniqueIsEmittedOnlyWhenTrue() {
		assertEquals("db.MDM_CUSTOMER.createIndex({ \"EMAIL\": 1, \"CUSTOMER_ID\": 1 }, "
						+ "{ name: \"EMAIL_1_CUSTOMER_ID_1\", unique: true, background: true })",
				ServingIndexManualCommands.mongo("MDM_CUSTOMER",
						index("EMAIL_1_CUSTOMER_ID_1", true, field("EMAIL", true), field("CUSTOMER_ID", true))));
	}

	@Test
	@DisplayName("点号路径字段必须加引号，否则是非法 JS")
	void dottedPathFieldIsQuoted() {
		assertEquals("db.MDM_CUSTOMER.createIndex({ \"POLICY.POLICY_STATUS\": 1 }, "
						+ "{ name: \"POLICY.POLICY_STATUS_1\", background: true })",
				ServingIndexManualCommands.mongo("MDM_CUSTOMER",
						index("POLICY.POLICY_STATUS_1", false, field("POLICY.POLICY_STATUS", true))));
	}

	@Test
	@DisplayName("没有字段的声明不出语句——建不出来的东西不能给人去执行")
	void indexWithoutFieldsYieldsNoCommand() {
		assertNull(ServingIndexManualCommands.mongo("MDM_CUSTOMER",
				new ServingIndex("broken", false, new ArrayList<>())));
		assertNull(ServingIndexManualCommands.mongo("MDM_CUSTOMER", null));
		assertNull(ServingIndexManualCommands.mongo("  ", index("CITY_1", false, field("CITY", true))));
	}

	@Test
	@DisplayName("名字缺失时不写 name，让 MongoDB 自己取——不能凭空编一个")
	void missingNameOmitsTheOption() {
		assertEquals("db.MDM_CUSTOMER.createIndex({ \"CITY\": 1 }, { background: true })",
				ServingIndexManualCommands.mongo("MDM_CUSTOMER",
						new ServingIndex(null, false,
								new ArrayList<>(Collections.singletonList(field("CITY", true))))));
	}

	@Test
	@DisplayName("只有 MongoDB 出语句，其它数据源一概不出")
	void onlyMongoDbGetsCommands() {
		ServingIndex index = index("CITY_1", false, field("CITY", true));
		assertEquals("db.MDM_CUSTOMER.createIndex({ \"CITY\": 1 }, { name: \"CITY_1\", background: true })",
				ServingIndexManualCommands.of("MongoDB", "MDM_CUSTOMER", index));
		assertEquals("db.MDM_CUSTOMER.createIndex({ \"CITY\": 1 }, { name: \"CITY_1\", background: true })",
				ServingIndexManualCommands.of("mongodb", "MDM_CUSTOMER", index), "类型比对不该大小写敏感");
		assertNull(ServingIndexManualCommands.of("Mysql", "MDM_CUSTOMER", index));
		assertNull(ServingIndexManualCommands.of(null, "MDM_CUSTOMER", index));
	}
}

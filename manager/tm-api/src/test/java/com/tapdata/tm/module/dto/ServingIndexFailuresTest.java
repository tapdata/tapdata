package com.tapdata.tm.module.dto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3-5 · 落地失败归类（{@link ServingIndexFailures}）。TAP-12057（方案 §4 风险表）。
 *
 * <p>把驱动原文原样丢给人等于没说：prod 上最常见的那一种失败是「目标账号只有读写权限、没有 DDL」，
 * 而它长成 {@code Command failed with error 13 (Unauthorized)} —— 看的人不知道要去改什么。
 * 归类只认两件事：<b>权限不足</b>（要人去授权）与<b>连接器不支持</b>（不算部署失败），
 * 其余一律 {@code UNKNOWN} 且<b>原样保留</b>原文——猜错比不猜更坏。</p>
 */
class ServingIndexFailuresTest {

	@Test
	@DisplayName("MongoDB 的 not authorized → 权限不足（这是 prod 只读写账号最常见的那一条）")
	void mongoNotAuthorizedIsPermissionDenied() {
		String driver = "Command failed with error 13 (Unauthorized): 'not authorized on tapdata to execute "
				+ "command { createIndexes: \"CUSTOMER\" }' on server 10.0.0.9:27017";

		assertEquals(ServingIndexFailures.Kind.PERMISSION_DENIED, ServingIndexFailures.classify(driver));
	}

	@Test
	@DisplayName("只剩 errmsg 正文（没有 (Unauthorized) 那段）也要认出来——包装过的异常常常只剩这一句")
	void bareNotAuthorizedTextIsPermissionDenied() {
		assertEquals(ServingIndexFailures.Kind.PERMISSION_DENIED, ServingIndexFailures.classify(
				"not authorized on mdm to execute command { createIndexes: \"CUSTOMER\" }"));
	}

	@Test
	@DisplayName("其他方言的权限措辞同样认（连接器是 PDK 抽象，不止 Mongo 一种目标）")
	void otherPermissionWordingIsRecognised() {
		assertEquals(ServingIndexFailures.Kind.PERMISSION_DENIED,
				ServingIndexFailures.classify("ERROR 1142: INDEX command denied to user 'app'@'10.0.0.9'"));
		assertEquals(ServingIndexFailures.Kind.PERMISSION_DENIED,
				ServingIndexFailures.classify("Access denied for user 'app'"));
		assertEquals(ServingIndexFailures.Kind.PERMISSION_DENIED,
				ServingIndexFailures.classify("permission denied for relation customer"));
	}

	@Test
	@DisplayName("连接器不支持建索引 → UNSUPPORTED（方案 §4：记 skipped-unsupported，不算部署失败）")
	void connectorWithoutCreateIndexIsUnsupported() {
		assertEquals(ServingIndexFailures.Kind.UNSUPPORTED,
				ServingIndexFailures.classify("Connector does not support create index"));
	}

	@Test
	@DisplayName("认不出的驱动错误保持 UNKNOWN——不硬套归类（唯一约束违约就该是它自己的样子）")
	void unrelatedDriverErrorStaysUnknown() {
		assertEquals(ServingIndexFailures.Kind.UNKNOWN, ServingIndexFailures.classify(
				"E11000 duplicate key error collection: mdm.CUSTOMER index: a_1 dup key: { a: 1 }"));
	}

	@Test
	@DisplayName("空/null 不炸，按 UNKNOWN 处理")
	void blankMessageIsUnknown() {
		assertEquals(ServingIndexFailures.Kind.UNKNOWN, ServingIndexFailures.classify(null));
		assertEquals(ServingIndexFailures.Kind.UNKNOWN, ServingIndexFailures.classify("   "));
	}

	@Test
	@DisplayName("explain 给出「要去做什么」，且驱动原文一个字不吞")
	void explainSaysWhatToDoAndKeepsTheDriverText() {
		String driver = "Command failed with error 13 (Unauthorized): 'not authorized on tapdata to execute "
				+ "command { createIndexes: \"CUSTOMER\" }'";

		String explained = ServingIndexFailures.explain(driver);

		assertTrue(explained.contains("createIndex"), explained);
		assertTrue(explained.toLowerCase().contains("privilege"), explained);
		assertTrue(explained.contains(driver), "驱动原文必须原样保留，排查时要靠它: " + explained);
	}

	@Test
	@DisplayName("不支持的连接器 explain 说明「按不支持跳过」")
	void explainMarksUnsupportedAsSkipped() {
		String explained = ServingIndexFailures.explain("Connector does not support create index");

		assertTrue(explained.toLowerCase().contains("unsupported"), explained);
		assertTrue(explained.contains("Connector does not support create index"), explained);
	}

	@Test
	@DisplayName("UNKNOWN 原样返回——不给认不出的错误加噪音")
	void explainLeavesUnknownUntouched() {
		String driver = "E11000 duplicate key error collection: mdm.CUSTOMER index: a_1";

		assertEquals(driver, ServingIndexFailures.explain(driver));
		assertEquals("", ServingIndexFailures.explain(null));
	}
}

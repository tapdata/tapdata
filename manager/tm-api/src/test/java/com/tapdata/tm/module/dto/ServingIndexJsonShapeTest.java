package com.tapdata.tm.module.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P2-1 · 服务型索引的导出 JSON 线格式必须最小、稳定（<b>ADR-0001</b>）。
 *
 * <p>存储/导出形态只允许 {@code {name, unique, fields:[{field, asc}]}}——绝不夹带 Mongo 原始元信息
 * （{@code v}/{@code ns}）或 PDK 模型多余字段（{@code type}/{@code subPosition}/{@code primary}/{@code cluster}），
 * 否则 Module 全量 diff 抖动。无索引的 Module 不落 {@code servingIndexes} 键（NON_EMPTY），避免既有 Module 误报变更。</p>
 */
class ServingIndexJsonShapeTest {

	private final ObjectMapper mapper = new ObjectMapper();

	private static Set<String> keys(JsonNode node) {
		Set<String> s = new LinkedHashSet<>();
		for (Iterator<String> it = node.fieldNames(); it.hasNext(); ) {
			s.add(it.next());
		}
		return s;
	}

	@Test
	@DisplayName("存储形态只序列化 {name, unique, fields:[{field, asc}]}，方向显式、无 v/ns/PDK 多余字段")
	void minimalWireShape() throws Exception {
		ServingIndex raw = new ServingIndex("ix_orders", true, Arrays.asList(
				new ServingIndexField("region", null),   // 升序默认，归一化后应为显式 true
				new ServingIndexField("ts", false)));
		List<ServingIndex> stored = ServingIndexNormalizer.normalize(Collections.singletonList(raw));
		String json = mapper.writeValueAsString(stored.get(0));

		JsonNode node = mapper.readTree(json);
		assertEquals(new LinkedHashSet<>(Arrays.asList("name", "unique", "fields")), keys(node));
		assertEquals("ix_orders", node.get("name").asText());
		assertTrue(node.get("unique").asBoolean());
		assertEquals(2, node.get("fields").size());
		assertEquals(new LinkedHashSet<>(Arrays.asList("field", "asc")), keys(node.get("fields").get(0)));
		assertEquals("region", node.get("fields").get(0).get("field").asText());
		assertTrue(node.get("fields").get(0).get("asc").asBoolean(), "升序方向应显式落为 asc:true");
		assertFalse(node.get("fields").get(1).get("asc").asBoolean(), "降序方向应为 asc:false");

		for (String forbidden : new String[]{"\"v\"", "\"ns\"", "\"type\"", "\"subPosition\"", "\"primary\"", "\"cluster\""}) {
			assertFalse(json.contains(forbidden), "线格式不应含 " + forbidden + "：" + json);
		}
	}

	@Test
	@DisplayName("无索引的 Module 不落 servingIndexes 键（NON_EMPTY）")
	void absentWhenEmpty() throws Exception {
		ModulesDto dto = new ModulesDto();
		dto.setName("api1");
		assertFalse(mapper.writeValueAsString(dto).contains("servingIndexes"),
				"servingIndexes 为空时不应出现在 Module JSON");
	}
}

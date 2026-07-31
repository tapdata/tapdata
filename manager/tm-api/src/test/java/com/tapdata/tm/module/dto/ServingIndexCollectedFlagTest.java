package com.tapdata.tm.module.dto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TAP-12057 · {@code ServingIndex.collected}：区分「本 API 声明要用」与「只是读回后留在列表里」。
 *
 * <p>为什么要这个标志：用户希望 load + save 之后，<b>没勾选的物理索引下次打开也还在列表里</b>
 * （2026-07-31 用户决定），因此 Module 里存的不再只是声明，还有「知道有这么条索引」。
 * 于是必须有一个位来分辨——**只有 {@code collected=true} 才是声明**：</p>
 * <ul>
 *   <li>部署（P3）只创建 {@code collected=true} 的；</li>
 *   <li>归因「已被本/他 API 收录」只认 {@code collected=true} 的；</li>
 *   <li>历史数据没有这个字段（此前只存勾选项）→ {@code null} 一律按 <b>true</b> 解释，语义不变。</li>
 * </ul>
 */
class ServingIndexCollectedFlagTest {

	private static ServingIndex index(String name, Boolean collected) {
		ServingIndex si = new ServingIndex(name, false,
				new ArrayList<>(Collections.singletonList(new ServingIndexField(name, true))));
		si.setCollected(collected);
		return si;
	}

	@Test
	@DisplayName("历史数据无该字段（null）→ 按已收录解释，语义与旧版一致")
	void nullMeansCollected() {
		assertTrue(ServingIndexes.isCollected(index("a", null)));
	}

	@Test
	@DisplayName("显式 true / false 各按其面值")
	void explicitFlag() {
		assertTrue(ServingIndexes.isCollected(index("a", Boolean.TRUE)));
		assertFalse(ServingIndexes.isCollected(index("a", Boolean.FALSE)));
	}

	@Test
	@DisplayName("collected 过滤：只留声明项——部署/归因/导出共用这一条口径")
	void collectedOnlyFilter() {
		List<ServingIndex> all = Arrays.asList(
				index("declared", Boolean.TRUE),
				index("known_only", Boolean.FALSE),
				index("legacy", null));

		List<ServingIndex> collected = ServingIndexes.collectedOnly(all);

		assertEquals(2, collected.size(), "未勾选项不得进入声明面");
		assertEquals("declared", collected.get(0).getName());
		assertEquals("legacy", collected.get(1).getName());
	}

	@Test
	@DisplayName("归一化保留随附展示信息：归因标签同样不得在拷贝中丢失")
	void normalizerKeepsAttribution() {
		ServingIndex si = index("a", Boolean.FALSE);
		si.setAttribution("COLLECTED_BY_OTHER_API");
		si.setAttributionApi("other-api");

		ServingIndex out = ServingIndexNormalizer
				.normalize(new ArrayList<>(Collections.singletonList(si)))
				.get(0);

		assertEquals("COLLECTED_BY_OTHER_API", out.getAttribution());
		assertEquals("other-api", out.getAttributionApi());
		assertEquals(Boolean.FALSE, out.getCollected());
	}

	@Test
	@DisplayName("归一化保留 collected：排序/方向规范化不得把标志丢了")
	void normalizerKeepsFlag() {
		List<ServingIndex> normalized = ServingIndexNormalizer.normalize(new ArrayList<>(Arrays.asList(
				index("b", Boolean.FALSE),
				index("a", Boolean.TRUE))));

		assertEquals("a", normalized.get(0).getName());
		assertEquals(Boolean.TRUE, normalized.get(0).getCollected());
		assertEquals("b", normalized.get(1).getName());
		assertEquals(Boolean.FALSE, normalized.get(1).getCollected());
	}
}

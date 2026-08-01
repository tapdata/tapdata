package com.tapdata.tm.module.dto;

import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
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
 * P3-2 · 落地侧自写索引比对（{@link ServingIndexLandingPlanner}）。
 *
 * <p>方案 §3.4 冲突口径 / <b>ADR-0005</b>：<b>按「有序字段+方向」配对</b>，名字与 {@code unique} 均不参与；
 * 动作只有「创建 / 跳过」两种，目标多出的索引<b>只列出、绝不删</b>。两条实现红线在此钉死：
 * <b>禁复用引擎 {@code tapIndexEquals}</b>（{@code compareIndexName=true} 时按名短路、取向相反），
 * <b>禁依赖连接器 errorCode 85/86</b>（被 catch 后 continue、调用方收到"成功"）——判定必须由前置
 * {@code QueryIndexes} 的自写比对完成。</p>
 */
class ServingIndexLandingPlannerTest {

	private static ServingIndexField f(String field, Boolean asc) {
		return new ServingIndexField(field, asc);
	}

	private static ServingIndex idx(String name, Boolean unique, ServingIndexField... fields) {
		return new ServingIndex(name, unique, new ArrayList<>(Arrays.asList(fields)));
	}

	private static TapIndexField tif(String name, Boolean asc) {
		return new TapIndexField().name(name).fieldAsc(asc);
	}

	private static TapIndex tapIndex(String name, Boolean unique, TapIndexField... fields) {
		TapIndex index = new TapIndex().name(name);
		if (unique != null) {
			index.unique(unique);
		}
		for (TapIndexField field : fields) {
			index.indexField(field);
		}
		return index;
	}

	private static List<ServingIndex> declared(ServingIndex... indexes) {
		return new ArrayList<>(Arrays.asList(indexes));
	}

	private static List<TapIndex> existing(TapIndex... indexes) {
		return new ArrayList<>(Arrays.asList(indexes));
	}

	@Test
	@DisplayName("目标无同字段集索引 → 创建；名字用推导名，不用声明里的展示名")
	void createsWhenTargetHasNoMatchingFieldSet() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("源环境里人起的名", null, f("a", true), f("b", false))),
				existing(tapIndex("_id_", null, tif("_id", true))));

		assertEquals(1, plan.getCreate().size());
		assertEquals("a_1_b_-1", plan.getCreate().get(0).getName(), "建索引一律用确定性推导名（§3.4）");
		assertTrue(plan.getSkip().isEmpty());
	}

	@Test
	@DisplayName("目标已有同「有序字段+方向」索引（名字不同）→ 跳过，不重复创建")
	void skipsWhenTargetAlreadyHasSameIdentityUnderAnotherName() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("声明名", null, f("a", true))),
				existing(tapIndex("dba_起的完全不同的名", null, tif("a", true))));

		assertTrue(plan.getCreate().isEmpty(), "名字不参与身份 → 异名同字段集必须判命中");
		assertEquals(1, plan.getSkip().size());
		assertEquals("dba_起的完全不同的名", plan.getSkip().get(0).getExisting().getName(),
				"报告要给出目标里真实存在的那条索引名");
	}

	@Test
	@DisplayName("目标有同名但字段不同的索引 → 不算命中，仍要创建（禁 tapIndexEquals 按名短路）")
	void sameNameDifferentFieldsIsNotAMatch() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("ix_shared", null, f("a", true))),
				existing(tapIndex("ix_shared", null, tif("zzz", true))));

		assertEquals(1, plan.getCreate().size(), "同名异字段被 tapIndexEquals 判等 → 本方案必须判不等");
		assertEquals("a_1", plan.getCreate().get(0).getName());
	}

	@Test
	@DisplayName("方向不同即两条索引：声明 {a:-1}、目标 {a:1} → 创建")
	void directionIsPartOfIdentity() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("ix", null, f("a", false))),
				existing(tapIndex("a_1", null, tif("a", true))));

		assertEquals(1, plan.getCreate().size());
		assertEquals("a_-1", plan.getCreate().get(0).getName());
	}

	@Test
	@DisplayName("复合索引字段顺序是语义：声明 {a,b}、目标 {b,a} → 创建")
	void compoundFieldOrderIsPartOfIdentity() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("ix", null, f("a", true), f("b", true))),
				existing(tapIndex("b_1_a_1", null, tif("b", true), tif("a", true))));

		assertEquals(1, plan.getCreate().size());
		assertEquals("a_1_b_1", plan.getCreate().get(0).getName());
	}

	@Test
	@DisplayName("目标多出的索引 → 只列进 extra 桶（只加不删，绝不出现在创建/跳过里）")
	void targetOnlyIndexesAreListedNeverDropped() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("ix", null, f("a", true))),
				existing(tapIndex("a_1", null, tif("a", true)),
						tapIndex("dba_调优_索引", null, tif("tuning", false))));

		assertEquals(1, plan.getExtra().size());
		assertEquals("dba_调优_索引", plan.getExtra().get(0).getName());
		assertTrue(plan.getCreate().isEmpty());
	}

	@Test
	@DisplayName("unique 不一致仍算命中 → 跳过，只在报告里作信息注记（不叫冲突、不影响部署结果）")
	void uniqueMismatchIsNotedButStillSkipped() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("ix", true, f("a", true))),
				existing(tapIndex("a_1", false, tif("a", true))));

		assertTrue(plan.getCreate().isEmpty(), "unique 不参与身份 → 必须判命中");
		assertEquals(1, plan.getSkip().size());
		assertTrue(plan.getSkip().get(0).isUniqueMismatch(), "unique 不一致要注记出来");
	}

	@Test
	@DisplayName("unique 一致（含 null 视同 false）→ 跳过且无注记")
	void uniqueMatchNeedsNoNote() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("ix", null, f("a", true))),
				existing(tapIndex("a_1", false, tif("a", true))));

		assertEquals(1, plan.getSkip().size());
		assertFalse(plan.getSkip().get(0).isUniqueMismatch(), "声明未写 unique = 非唯一，与目标一致");
	}

	@Test
	@DisplayName("多 API 共表：同字段集的两份声明并集去重 → 只创建一条，unique 取更严者 true（§5.5）")
	void mergesDuplicateDeclarationsTakingStricterUnique() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("来自 API A", null, f("a", true)),
						idx("来自 API B", true, f("a", true))),
				existing());

		assertEquals(1, plan.getCreate().size(), "同字段集必得同名，建两次是自撞");
		assertEquals(Boolean.TRUE, plan.getCreate().get(0).getUnique(),
				"建失败是响亮的 11000，好过静默丢约束");
	}

	@Test
	@DisplayName("多 API 共表且目标已有：并集去重后跳过也只出一条")
	void mergesDuplicateDeclarationsWhenSkipping() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("来自 API A", null, f("a", true)),
						idx("来自 API B", null, f("a", true))),
				existing(tapIndex("a_1", null, tif("a", true))));

		assertEquals(1, plan.getSkip().size());
	}

	@Test
	@DisplayName("未勾选（collected=false）的不是声明：既不创建，目标上的同名索引按「多出」列出")
	void uncollectedEntriesAreNotDeclarations() {
		ServingIndex uncollected = idx("读回后没勾的", null, f("a", true));
		uncollected.setCollected(false);

		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(uncollected),
				existing(tapIndex("a_1", null, tif("a", true))));

		assertTrue(plan.getCreate().isEmpty(), "没勾的不能去建（ADR-0011 D2）");
		assertTrue(plan.getSkip().isEmpty());
		assertEquals(1, plan.getExtra().size(), "无对应声明 → 落「目标多出」桶，仅列出");
	}

	@Test
	@DisplayName("collected=null 按已收录解释（历史数据只存勾选项）→ 照常创建")
	void nullCollectedIsTreatedAsDeclared() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(idx("历史数据", null, f("a", true))),
				existing());

		assertEquals(1, plan.getCreate().size());
	}

	@Test
	@DisplayName("无字段的声明推不出索引 → 不创建（空签名不是身份）")
	void declarationWithoutFieldsIsNotCreatable() {
		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(
				declared(new ServingIndex("空声明", null, null)),
				existing());

		assertTrue(plan.getCreate().isEmpty());
	}

	@Test
	@DisplayName("null / 空入参安全：无声明时目标索引全进 extra，不创建任何东西")
	void nullSafe() {
		ServingIndexLandingPlan empty = ServingIndexLandingPlanner.plan(null, null);
		assertTrue(empty.getCreate().isEmpty());
		assertTrue(empty.getSkip().isEmpty());
		assertTrue(empty.getExtra().isEmpty());

		ServingIndexLandingPlan noDeclaration = ServingIndexLandingPlanner.plan(
				Collections.emptyList(), existing(tapIndex("a_1", null, tif("a", true))));
		assertTrue(noDeclaration.getCreate().isEmpty());
		assertEquals(1, noDeclaration.getExtra().size());
	}
}

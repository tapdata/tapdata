package io.tapdata.flow.engine.V2.index;

import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * 仅供 {@link IndexPackageRedlineArchTest} 的 teeth 用例：故意<b>只</b>依赖 {@link TwoDbRedline}
 * （声称自己守着两库红线）与 {@code MongoTemplate}（TM 平台自有库直连），<b>不</b>依赖
 * {@link PdkIndexService}。
 *
 * <p>这正是 P3 收尾把节点构建抽进 {@code ConnectionScopedPdkHandler} 后出现的形状——基类持有
 * 红线断言与 PDK 节点构建，却不再直接碰 {@code PdkIndexService}，于是从「依赖 PdkIndexService」
 * 那条判据里漏了出去。位于 test 源树，主编译期扫描（{@code DoNotIncludeTests}）不收录它。</p>
 */
class RedlineGuardViolatorFixture {
	MongoTemplate mongoTemplate;

	void guard(String targetUri, String platformUri) {
		TwoDbRedline.assertTargetIsUserDb(targetUri, platformUri);
	}
}

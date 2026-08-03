package io.tapdata.flow.engine.V2.index;

import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * 仅供 {@link IndexPackageRedlineArchTest} 的 teeth 用例：<b>故意</b>同时依赖
 * {@link PdkIndexService}（索引读写通道）与 {@code MongoTemplate}（TM 平台自有库直连）
 * ——「驱动索引通道的类却连了平台库」这一违规样本（ADR-0002）。
 * 位于 test 源树，主编译期 ArchUnit 扫描（{@code DoNotIncludeTests}）不收录它。
 */
class IndexChannelUserViolatorFixture {
	PdkIndexService pdkIndexService;
	MongoTemplate mongoTemplate;
}

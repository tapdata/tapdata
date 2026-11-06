package com.tapdata.tm.servingindex;

import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * teeth 用例（仅测试期）：故意在 {@code com.tapdata.tm.servingindex} 包内依赖 {@link MongoTemplate}，
 * 证明 {@link ServingIndexPackageRedlineArchTest} 的红线规则确实拦得住。绝不在主源出现。
 */
class RedlineViolatorFixture {

	@SuppressWarnings("unused")
	private MongoTemplate mongoTemplate;
}

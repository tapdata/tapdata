package com.tapdata.tm.servingindex;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * P1-2 · TM 侧两库红线（编译期，TAP-12057 / ADR-0002）——与引擎 {@code IndexPackageRedlineArchTest} 对称。
 *
 * <p>服务型索引本体只经引擎 PDK 连接器落<b>用户库</b>；TM 侧索引服务包（{@code com.tapdata.tm.servingindex..}）
 * <b>绝不</b>直接依赖 {@code MongoTemplate}——它直连平台自有库、错库写入不报错（反面教材见 ADR-0002）。
 * 连接元信息、worker 解析等一律走既有 service 接口，不在本包裸用 {@code MongoTemplate}。</p>
 */
class ServingIndexPackageRedlineArchTest {

	private static final String MONGO_TEMPLATE = "org.springframework.data.mongodb.core.MongoTemplate";

	/** TM 索引服务包禁依赖 MongoTemplate。落地写动作（p3）随其落盘后并入本 package 匹配。 */
	private static final ArchRule NO_MONGO_TEMPLATE = noClasses()
			.that().resideInAPackage("com.tapdata.tm.servingindex..")
			.should().dependOnClassesThat().haveFullyQualifiedName(MONGO_TEMPLATE)
			.because("服务型索引本体只经 PDK 连接器落用户库；TM MongoTemplate 直连平台自有库、错库写入不报错（ADR-0002）");

	@Test
	@DisplayName("真实 TM 索引服务包（主源）不依赖 MongoTemplate → 红线通过")
	void servingIndexPackage_isCleanOfMongoTemplate() {
		JavaClasses mainClasses = new ClassFileImporter()
				.withImportOption(new ImportOption.DoNotIncludeTests())
				.importPackages("com.tapdata.tm.servingindex");
		assertDoesNotThrow(() -> NO_MONGO_TEMPLATE.check(mainClasses));
	}

	@Test
	@DisplayName("teeth：故意在索引服务包内依赖 MongoTemplate → 红线响亮失败")
	void deliberateMongoTemplateDependency_isCaught() {
		JavaClasses withViolator = new ClassFileImporter()
				.importClasses(RedlineViolatorFixture.class, MongoTemplate.class);
		assertThrows(AssertionError.class, () -> NO_MONGO_TEMPLATE.check(withViolator));
	}
}

package io.tapdata.flow.engine.V2.index;

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
 * P1-3 · 两库红线（编译期，TAP-12057 / ADR-0002）。
 *
 * <p>服务型索引本体只经引擎 PDK 连接器落<b>用户库</b>；索引服务/落地包<b>绝不</b>依赖 TM 的
 * {@code MongoTemplate}（它直连平台自有库、错库写入不报错）。本规则把该红线结构化为编译期断言。</p>
 */
class IndexPackageRedlineArchTest {

	private static final String MONGO_TEMPLATE = "org.springframework.data.mongodb.core.MongoTemplate";

	/** 索引/落地包禁依赖 MongoTemplate。落地包（p3）随其落盘后并入本 package 匹配。 */
	private static final ArchRule NO_MONGO_TEMPLATE = noClasses()
			.that().resideInAPackage("io.tapdata.flow.engine.V2.index..")
			.should().dependOnClassesThat().haveFullyQualifiedName(MONGO_TEMPLATE)
			.because("服务型索引本体只经 PDK 连接器落用户库；MongoTemplate 直连平台自有库、错库写入不报错（ADR-0002）");

	@Test
	@DisplayName("真实索引包（主源）不依赖 MongoTemplate → 红线通过")
	void indexPackage_isCleanOfMongoTemplate() {
		JavaClasses mainClasses = new ClassFileImporter()
				.withImportOption(new ImportOption.DoNotIncludeTests())
				.importPackages("io.tapdata.flow.engine.V2.index");
		assertDoesNotThrow(() -> NO_MONGO_TEMPLATE.check(mainClasses));
	}

	@Test
	@DisplayName("teeth：故意在索引包内依赖 MongoTemplate → 红线响亮失败")
	void deliberateMongoTemplateDependency_isCaught() {
		JavaClasses withViolator = new ClassFileImporter()
				.importClasses(RedlineViolatorFixture.class, MongoTemplate.class);
		assertThrows(AssertionError.class, () -> NO_MONGO_TEMPLATE.check(withViolator));
	}
}

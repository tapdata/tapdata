package io.tapdata.flow.engine.V2.index;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P1-3 · 两库红线（编译期，TAP-12057 / ADR-0002）。
 *
 * <p>服务型索引本体只经引擎 PDK 连接器落<b>用户库</b>；索引服务/落地包<b>绝不</b>依赖 TM 的
 * {@code MongoTemplate}（它直连平台自有库、错库写入不报错）。本规则把该红线结构化为编译期断言。</p>
 */
class IndexPackageRedlineArchTest {

	private static final String MONGO_TEMPLATE = "org.springframework.data.mongodb.core.MongoTemplate";
	private static final String PDK_INDEX_SERVICE = "io.tapdata.flow.engine.V2.index.PdkIndexService";
	private static final String TWO_DB_REDLINE = "io.tapdata.flow.engine.V2.index.TwoDbRedline";

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

	/**
	 * 上面那条按<b>包</b>匹配，覆盖不到包外的索引通道使用者——{@code QueryIndexesHandler} 与
	 * {@code CreateIndexHandler} 都在 {@code io.tapdata.websocket.handler}（P1 起就有的缺口）。
	 * 那个包里混着大量无关 handler、不能整包封，故改<b>按结构</b>匹配，不必维护类名清单、
	 * 将来新增的使用者自动被罩住。判据认两条结构特征，命中其一即入网：
	 *
	 * <ul>
	 *   <li><b>驱动</b>——依赖 {@link PdkIndexService}（真去读/建索引）；</li>
	 *   <li><b>守卫</b>——依赖 {@link TwoDbRedline}（自称守着两库红线，即处在索引落库路径上）。</li>
	 * </ul>
	 *
	 * <p>第二条是 P3 收尾补的：把 PDK 节点构建抽进 {@code ConnectionScopedPdkHandler} 之后，该基类持有
	 * 红线断言与建节点逻辑、却不再直接碰 {@code PdkIndexService}，只认第一条就会从网里漏出去
	 * （teeth 见 {@code redlineGuardTouchingMongoTemplate_isCaught}）。</p>
	 */
	private static final DescribedPredicate<JavaClass> DRIVE_INDEX_CHANNEL =
			new DescribedPredicate<JavaClass>("依赖 PdkIndexService 或 TwoDbRedline（驱动或守卫索引读写通道）") {
				@Override
				public boolean test(JavaClass javaClass) {
					return javaClass.getDirectDependenciesFromSelf().stream()
							.map(dep -> dep.getTargetClass().getFullName())
							.anyMatch(target -> PDK_INDEX_SERVICE.equals(target) || TWO_DB_REDLINE.equals(target));
				}
			};

	private static final ArchRule INDEX_CHANNEL_USERS_NO_MONGO_TEMPLATE = noClasses()
			.that(DRIVE_INDEX_CHANNEL)
			.should().dependOnClassesThat().haveFullyQualifiedName(MONGO_TEMPLATE)
			.because("驱动索引读写通道的类只能经 PDK 连接器作用于用户库；MongoTemplate 直连平台自有库（ADR-0002）");

	@Test
	@DisplayName("全仓扫：所有索引通道使用者（含两个 ws handler）都不依赖 MongoTemplate")
	void everyIndexChannelUser_isCleanOfMongoTemplate() {
		JavaClasses engineClasses = new ClassFileImporter()
				.withImportOption(new ImportOption.DoNotIncludeTests())
				.importPackages("io.tapdata");
		assertDoesNotThrow(() -> INDEX_CHANNEL_USERS_NO_MONGO_TEMPLATE.check(engineClasses));
	}

	@Test
	@DisplayName("teeth：既用索引通道又连平台库 → 红线响亮失败")
	void indexChannelUserTouchingMongoTemplate_isCaught() {
		JavaClasses withViolator = new ClassFileImporter()
				.importClasses(IndexChannelUserViolatorFixture.class, PdkIndexService.class, MongoTemplate.class);

		AssertionError caught = assertThrows(AssertionError.class,
				() -> INDEX_CHANNEL_USERS_NO_MONGO_TEMPLATE.check(withViolator));
		assertTrue(caught.getMessage().contains(IndexChannelUserViolatorFixture.class.getSimpleName()),
				"应点名违规类，实际失败信息：" + caught.getMessage());
	}

	@Test
	@DisplayName("teeth：只守红线、不碰 PdkIndexService 的类（如公共基类）连了平台库 → 同样要被抓住")
	void redlineGuardTouchingMongoTemplate_isCaught() {
		// P3 收尾把 PDK 节点构建抽进 ConnectionScopedPdkHandler 后，该基类不再直接依赖 PdkIndexService，
		// 单靠「依赖 PdkIndexService」那条判据就会漏——判据须同时认「依赖 TwoDbRedline」这条结构特征。
		JavaClasses withViolator = new ClassFileImporter()
				.importClasses(RedlineGuardViolatorFixture.class, TwoDbRedline.class, MongoTemplate.class);

		AssertionError caught = assertThrows(AssertionError.class,
				() -> INDEX_CHANNEL_USERS_NO_MONGO_TEMPLATE.check(withViolator));
		// 必须因「抓到违规」而失败，而不是因为判据一个类都没匹配上（ArchUnit 的 empty-should 也抛 AssertionError，
		// 不钉住原因就会拿「规则根本没覆盖到它」当成「规则抓住了它」）。
		assertTrue(caught.getMessage().contains(RedlineGuardViolatorFixture.class.getSimpleName()),
				"应点名违规类，实际失败信息：" + caught.getMessage());
	}
}

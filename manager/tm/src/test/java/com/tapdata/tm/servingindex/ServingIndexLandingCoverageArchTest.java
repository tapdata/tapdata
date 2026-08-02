package com.tapdata.tm.servingindex;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>每一条导入模块的腿都要落地索引</b>。TAP-12057 · P3-1/P3-3。
 *
 * <p><b>为什么需要这条测试</b>：2026-08-01 实机没看到落地日志 —— 方案与 plan 给的落点行号只指向
 * {@code executeImportApisStandaloneAsync}，我照做了，却漏了 {@code executeImportAsync}（整包导入，
 * CICD 部署真正走的那条）。两条腿都导 Module、都有真 conMap，只接一条 = 部署时索引根本不建，
 * 而且<b>不报错</b>——正是本工单要消灭的那种「静默的谎」。</p>
 *
 * <p>规则：{@code GroupInfoService} 里任何调用 {@code ModulesService.batchImport} 的方法，
 * 都必须同时调用 {@code ServingIndexLandingService.landAfterImport}。新增导入腿时这条会立刻转红。</p>
 */
class ServingIndexLandingCoverageArchTest {

	private static final String MODULES_SERVICE = "com.tapdata.tm.modules.service.ModulesService";
	private static final String LANDING_SERVICE = "com.tapdata.tm.servingindex.ServingIndexLandingService";

	/**
	 * 已知豁免：{@code executeImportApisByNames} 那条腿传的是<b>空 conMap</b>（见其调用点），
	 * 目标连接无从解析，本期不接（TODO {@code P3-import-leg-scope}）。豁免写在这里而不是删规则,
	 * 这样它是一条<b>有据可查的决定</b>，不是一个漏洞。
	 */
	private static final Set<String> EXEMPT = Set.of("GroupInfoService#executeImportApisAsync");

	private static boolean calls(JavaMethod method, String ownerFqn, String methodName) {
		for (JavaMethodCall call : method.getMethodCallsFromSelf()) {
			if (call.getTargetOwner().getName().equals(ownerFqn)
					&& call.getName().equals(methodName)) {
				return true;
			}
		}
		return false;
	}

	@Test
	@DisplayName("每个导入 Module 的方法都要触发索引落地（豁免须显式登记）")
	void everyModuleImportLegLandsServingIndexes() {
		// 全仓扫，不只 GroupInfoService——2026-08-01 第二次实机就是栽在这：第四条腿
		// ModulesService#batchUpTask（API 列表页导入）在另一个类里，只扫一个类的守卫看不见它。
		JavaClasses classes = new ClassFileImporter()
				.withImportOption(new ImportOption.DoNotIncludeTests())
				.importPackages("com.tapdata.tm");

		List<String> importLegs = new ArrayList<>();
		List<String> missing = new ArrayList<>();
		for (JavaClass type : classes) {
			for (JavaMethod method : type.getMethods()) {
				if (!calls(method, MODULES_SERVICE, "batchImport")) {
					continue;
				}
				String leg = type.getSimpleName() + "#" + method.getName();
				importLegs.add(leg);
				if (EXEMPT.contains(leg) || calls(method, LANDING_SERVICE, "landAfterImport")) {
					continue;
				}
				missing.add(leg);
			}
		}

		assertFalse(importLegs.isEmpty(), "一条导入腿都没扫到说明匹配口径坏了，这条测试就成了摆设");
		assertTrue(missing.isEmpty(),
				"这些导入腿导了 Module 却不落地索引，部署会静默地不建索引：" + missing);
	}
}

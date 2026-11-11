package com.tapdata.tm.audit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.MethodMetadata;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 审计目录覆盖：TM 中每个写接口要么声明 @Audited，要么登记在排除清单并写明理由；新增写接口未登记时测试失败
 * （需求 BR-17 / 第 21 章发布准入）。同时校验每个 Action 的三语文案和命名约束。
 */
class AuditCatalogCoverageTest {
	private static final List<String> WRITE_MAPPINGS = List.of(
			"org.springframework.web.bind.annotation.PostMapping", "org.springframework.web.bind.annotation.PutMapping",
			"org.springframework.web.bind.annotation.PatchMapping", "org.springframework.web.bind.annotation.DeleteMapping");
	private static final String REQUEST_MAPPING = "org.springframework.web.bind.annotation.RequestMapping";
	private static final String AUDITED = Audited.class.getName();
	private static final List<String> BUNDLES = List.of("auditMessages.properties", "auditMessages_en_US.properties",
			"auditMessages_zh_CN.properties", "auditMessages_zh_TW.properties");

	@Test
	@DisplayName("每个写接口均已声明审计或在排除清单中登记理由，排除清单不含已失效条目")
	void should_audit_or_exclude_every_write_endpoint() throws IOException {
		Set<String> unaudited = new TreeSet<>();
		Set<String> controllers = new TreeSet<>();
		Set<String> existing = new TreeSet<>();
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		CachingMetadataReaderFactory factory = new CachingMetadataReaderFactory(resolver);
		for (Resource resource : resolver.getResources("classpath*:com/tapdata/tm/**/*Controller.class")) {
			if (!resource.getURL().toString().contains("/tm/target/classes/")) {
				continue;
			}
			MetadataReader reader = factory.getMetadataReader(resource);
			AnnotationMetadata meta = reader.getAnnotationMetadata();
			if (!meta.hasAnnotation("org.springframework.web.bind.annotation.RestController")
					&& !meta.hasAnnotation("org.springframework.stereotype.Controller")) {
				continue;
			}
			controllers.add(meta.getClassName());
			for (MethodMetadata method : meta.getDeclaredMethods()) {
				if (!isWrite(method)) {
					continue;
				}
				String key = meta.getClassName() + "#" + method.getMethodName();
				existing.add(key);
				if (!method.isAnnotated(AUDITED)) {
					unaudited.add(key);
				}
			}
		}
		assertThat(controllers).as("应扫描到 TM 控制器").isNotEmpty();

		Set<String> excludedMethods = new TreeSet<>();
		Set<String> excludedClasses = new TreeSet<>();
		List<String> missingReason = new ArrayList<>();
		for (String line : readLines("audit/catalog-exclusions.txt")) {
			String trimmed = line.trim();
			if (trimmed.isEmpty() || trimmed.startsWith("#")) {
				continue;
			}
			int comment = trimmed.indexOf("  # ");
			if (comment < 0 || trimmed.substring(comment + 4).isBlank()) {
				missingReason.add(trimmed);
				continue;
			}
			String entry = trimmed.substring(0, comment).trim();
			if (entry.endsWith("#*")) {
				excludedClasses.add(entry.substring(0, entry.length() - 2));
			} else {
				excludedMethods.add(entry);
			}
		}
		assertThat(missingReason).as("排除条目必须写明理由").isEmpty();

		List<String> uncovered = unaudited.stream()
				.filter(key -> !excludedMethods.contains(key) && !excludedClasses.contains(key.substring(0, key.indexOf('#'))))
				.toList();
		assertThat(uncovered).as("以下写接口既未声明 @Audited 也未登记排除理由，请在 AuditAction 登记事件并声明审计").isEmpty();

		List<String> stale = new ArrayList<>();
		excludedMethods.stream().filter(key -> !unaudited.contains(key)).forEach(stale::add);
		excludedClasses.stream().filter(cls -> !controllers.contains(cls)).forEach(stale::add);
		assertThat(stale).as("排除清单含已不存在或已审计的条目，请删除").isEmpty();
	}

	@Test
	@DisplayName("每个 Action、事件类型、结果与来源在四个文案文件中均有文案，Action 键不含 URL 或方法名")
	void should_have_messages_and_stable_action_keys() throws IOException {
		for (String bundle : BUNDLES) {
			Properties messages = load(bundle);
			for (AuditAction action : AuditAction.values()) {
				assertThat(messages.getProperty(action.messageKey())).as(bundle + " 缺少 " + action.messageKey()).isNotBlank();
			}
			for (AuditEventType type : AuditEventType.values()) {
				assertThat(messages.getProperty("audit_event_type_" + type.name().toLowerCase())).as(bundle + " 缺少事件类型 " + type).isNotBlank();
			}
			for (AuditOutcome outcome : AuditOutcome.values()) {
				assertThat(messages.getProperty("audit_outcome_" + outcome.name().toLowerCase())).as(bundle + " 缺少结果 " + outcome).isNotBlank();
			}
			for (AuditSource source : AuditSource.values()) {
				assertThat(messages.getProperty("audit_source_" + source.name().toLowerCase())).as(bundle + " 缺少来源 " + source).isNotBlank();
			}
		}
		Set<String> keys = new TreeSet<>();
		for (AuditAction action : AuditAction.values()) {
			assertThat(action.getKey()).as(action.name()).matches("[a-z_]+\\.[a-z_]+");
			assertThat(keys.add(action.getKey())).as("Action 键重复：" + action.getKey()).isTrue();
			assertThat(AuditAction.ofKey(action.getKey())).isSameAs(action);
		}
	}

	@Test
	@DisplayName("Workflow 不在审计目录中")
	void should_exclude_workflow_from_catalog() {
		for (AuditAction action : AuditAction.values()) {
			assertThat(action.getKey()).doesNotContain("workflow");
		}
	}

	private static boolean isWrite(MethodMetadata method) {
		for (String mapping : WRITE_MAPPINGS) {
			if (method.isAnnotated(mapping)) {
				return true;
			}
		}
		if (!method.isAnnotated(REQUEST_MAPPING)) {
			return false;
		}
		Map<String, Object> attributes = method.getAnnotationAttributes(REQUEST_MAPPING);
		Object methods = attributes == null ? null : attributes.get("method");
		if (methods instanceof Object[] array) {
			for (Object m : array) {
				String name = String.valueOf(m);
				if (name.endsWith("POST") || name.endsWith("PUT") || name.endsWith("PATCH") || name.endsWith("DELETE")) {
					return true;
				}
			}
		}
		return false;
	}

	private static Properties load(String name) throws IOException {
		Properties properties = new Properties();
		try (InputStream in = new ClassPathResource(name).getInputStream()) {
			properties.load(in);
		}
		return properties;
	}

	private static List<String> readLines(String name) throws IOException {
		try (InputStream in = new ClassPathResource(name).getInputStream()) {
			return new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList();
		}
	}
}

package com.tapdata.tm.servingindex;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本包的 Spring bean <b>装配得起来</b>——构造器不歧义。TAP-12057。
 *
 * <p><b>为什么需要这条测试</b>：2026-08-01 实机启动栽了一次 ——
 * {@code ServingIndexLandingService} 有「生产用」与「测试注入超时」两个构造器、都没标 {@code @Autowired}，
 * Spring 便退回去找无参构造器，容器起不来（{@code NoSuchMethodException: <init>()}）。
 * 单测一律直接 {@code new}，<b>绕过了容器的构造器选择</b>，所以 30 条测试全绿也照样炸。</p>
 *
 * <p>规则（Spring 的实际行为）：<b>恰好一个构造器</b>时隐式注入；<b>多个构造器</b>时必须恰好一个标
 * {@code @Autowired}，否则退回无参构造器。本测试对本包每个 {@code @Service}/{@code @Component} 施加它。</p>
 */
class ServingIndexBeanWiringTest {

	private static List<Class<?>> springBeansInPackage() {
		JavaClasses classes = new ClassFileImporter()
				.withImportOption(new ImportOption.DoNotIncludeTests())
				.importPackages("com.tapdata.tm.servingindex");
		List<Class<?>> beans = new ArrayList<>();
		for (JavaClass javaClass : classes) {
			Class<?> type = javaClass.reflect();
			if (type.isAnnotationPresent(Service.class) || type.isAnnotationPresent(Component.class)) {
				beans.add(type);
			}
		}
		return beans;
	}

	/** Spring 能不能挑出构造器：单构造器 → 能；多构造器 → 须恰好一个 {@code @Autowired}。 */
	private static boolean isResolvable(Class<?> type) {
		Constructor<?>[] constructors = type.getDeclaredConstructors();
		if (constructors.length == 1) {
			return true;
		}
		int annotated = 0;
		for (Constructor<?> constructor : constructors) {
			if (constructor.isAnnotationPresent(Autowired.class)) {
				annotated++;
			}
		}
		return annotated == 1;
	}

	@Test
	@DisplayName("本包每个 Spring bean 的构造器都能被容器挑出来（多构造器须标 @Autowired）")
	void everyBeanHasAResolvableConstructor() {
		List<Class<?>> beans = springBeansInPackage();
		assertFalse(beans.isEmpty(), "扫不到 bean 说明扫描口径坏了，这条测试就成了摆设");

		List<String> broken = new ArrayList<>();
		for (Class<?> bean : beans) {
			if (!isResolvable(bean)) {
				broken.add(bean.getSimpleName() + " 有 " + bean.getDeclaredConstructors().length
						+ " 个构造器却没有恰好一个标 @Autowired");
			}
		}
		assertTrue(broken.isEmpty(), "Spring 挑不出构造器，容器起不来：" + broken);
	}

	/** teeth：本测试真能抓到那个把 TM 拦在启动线外的形状。 */
	@Test
	@DisplayName("teeth：多构造器且都没标 @Autowired → 判为装配不起来")
	void catchesAmbiguousConstructors() {
		assertFalse(isResolvable(AmbiguousFixture.class));
		assertTrue(isResolvable(AnnotatedFixture.class));
		assertTrue(isResolvable(SingleConstructorFixture.class));
	}

	@SuppressWarnings("unused")
	static class AmbiguousFixture {
		AmbiguousFixture(String a) {
		}

		AmbiguousFixture(String a, String b) {
		}
	}

	@SuppressWarnings("unused")
	static class AnnotatedFixture {
		@Autowired
		AnnotatedFixture(String a) {
		}

		AnnotatedFixture(String a, String b) {
		}
	}

	@SuppressWarnings("unused")
	static class SingleConstructorFixture {
		SingleConstructorFixture(String a) {
		}
	}
}

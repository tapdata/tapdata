/**
 * @title: StateMachineBeanScanner
 * @description:
 * @author lk
 * @date 2021/8/10
 */
package com.tapdata.tm.statemachine.configuration;

import java.lang.annotation.Annotation;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.filter.AnnotationTypeFilter;

public class StateMachineBeanScanner extends ClassPathBeanDefinitionScanner {

	public StateMachineBeanScanner(BeanDefinitionRegistry registry) {
		super(registry);
	}

	public StateMachineBeanScanner(BeanDefinitionRegistry registry, boolean useDefaultFilters) {
		super(registry, useDefaultFilters);
	}

	public StateMachineBeanScanner(BeanDefinitionRegistry registry, boolean useDefaultFilters, Environment environment) {
		super(registry, useDefaultFilters, environment);
	}

	public StateMachineBeanScanner(BeanDefinitionRegistry registry, boolean useDefaultFilters, Environment environment, ResourceLoader resourceLoader) {
		super(registry, useDefaultFilters, environment, resourceLoader);
	}

	public void addIncludeAnnotationTypeFilter(Class<? extends Annotation> annotationType){
		addIncludeFilter(new AnnotationTypeFilter(annotationType));
	}

	public void scan(String basePackages){
		doScan(basePackages);
	}
}

package com.tapdata.constant;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;

/**
 * Util to get classes according to annotation on class in a specified package.
 *
 * @author eric
 * @date Aug 5, 2014 12:36:42 PM
 */
public class PkgAnnoUtil {
	/**
	 * <p>
	 * Scan class with specified annotation under specific packages, using util from spring.
	 * </p>
	 * <p>
	 * Sub package & inner class will be included.
	 * </p>
	 *
	 * @param pkgArray       an array of package path,
	 * @param annoClazzArray an array of annotation class,
	 * @return
	 */
	public static Set<BeanDefinition> getBeanSetWithAnno(List<String> pkgArray, List<Class<? extends Annotation>> annoClazzArray) {
		// prepare scanner, with each annotation,
		ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
		for (Class<? extends Annotation> annoclazz : annoClazzArray) {
			scanner.addIncludeFilter(new AnnotationTypeFilter(annoclazz));
		}

		Set<BeanDefinition> beanSet = null;
		// search with each package, and combine search result,
		for (String pkg : pkgArray) {
			if (beanSet == null) {
				beanSet = scanner.findCandidateComponents(pkg);
			} else {
				beanSet.addAll(scanner.findCandidateComponents(pkg));
			}
		}
		return beanSet;
	}
}

package com.tapdata.tm.utils;

import java.lang.annotation.Annotation;
import java.util.Map;
import org.springframework.context.ApplicationContext;

/**
 * @author samuel
 * @Description
 * @create 2020-11-19 15:36
 **/
public class SpringContextHelper {

	public static ApplicationContext applicationContext;

	public static <T> T getBean(Class<T> clazz) {
		if (clazz == null || applicationContext == null) {
			return null;
		}

		T bean = applicationContext.getBean(clazz);
		if (bean == null) {
			throw new NullPointerException("Cannot find class in application context");
		}
		return bean;
	}

	public static Map<String, Object> getBeansWithAnnotation(Class<? extends Annotation> annotationType){

		if (applicationContext != null){
			return applicationContext.getBeansWithAnnotation(annotationType);
		}

		return null;
	}
}

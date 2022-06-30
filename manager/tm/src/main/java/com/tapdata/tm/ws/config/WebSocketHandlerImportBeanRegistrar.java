/**
 * @title: StateMachineConfigurationImportBeanRegistrar
 * @description:
 * @author lk
 * @date 2021/8/10
 */
package com.tapdata.tm.ws.config;

import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.filter.AnnotationTypeFilter;

@Slf4j
public class WebSocketHandlerImportBeanRegistrar implements ImportBeanDefinitionRegistrar {

	public static final String WS_HANDLER_PACKAGE_NAME = "com/tapdata/tm/ws/handler";

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		ClassPathBeanDefinitionScanner classPathBeanDefinitionScanner = new ClassPathBeanDefinitionScanner(registry, false);
		classPathBeanDefinitionScanner.addIncludeFilter(new AnnotationTypeFilter(WebSocketMessageHandler.class));
		int scan = classPathBeanDefinitionScanner.scan(WS_HANDLER_PACKAGE_NAME);
		log.info("The number of webSocketHandler beans successfully registered: {}", scan);
	}
}

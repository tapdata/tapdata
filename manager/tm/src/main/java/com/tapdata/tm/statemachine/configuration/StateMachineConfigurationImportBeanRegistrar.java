/**
 * @title: StateMachineConfigurationImportBeanRegistrar
 * @description:
 * @author lk
 * @date 2021/8/10
 */
package com.tapdata.tm.statemachine.configuration;

import com.tapdata.tm.statemachine.annotation.StateMachineHandler;
import com.tapdata.tm.statemachine.annotation.StateMachineHandlers;
import static com.tapdata.tm.statemachine.utils.StateMachineProcessorManager.PACKAGE_NAME;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

public class StateMachineConfigurationImportBeanRegistrar implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		StateMachineBeanScanner stateMachineBeanScanner = new StateMachineBeanScanner(registry, false);
		stateMachineBeanScanner.addIncludeAnnotationTypeFilter(StateMachineHandler.class);
		stateMachineBeanScanner.addIncludeAnnotationTypeFilter(StateMachineHandlers.class);
		stateMachineBeanScanner.scan(PACKAGE_NAME);
	}
}

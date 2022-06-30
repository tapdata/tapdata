package com.tapdata.tm.statemachine.configuration;

import com.tapdata.tm.statemachine.StateMachine;
import com.tapdata.tm.statemachine.StateMachineConfigurer;
import com.tapdata.tm.statemachine.StateMachineImpl;
import com.tapdata.tm.statemachine.annotation.EnableStateMachine;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultBeanNameGenerator;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

@Slf4j
public class StateMachineBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {

		Map<String, Object> attrMap = importingClassMetadata.getAnnotationAttributes(EnableStateMachine.class.getName());
		if (attrMap == null || !AnnotationAttributes.fromMap(attrMap).getBoolean("enabled")) {
			return;
		}
		String className = importingClassMetadata.getClassName();
		try {
			Class<?> stateMachineConfigClass = ClassUtils.forName(className, ClassUtils.getDefaultClassLoader());
			if (ClassUtils.isAssignable(StateMachineConfigurer.class, stateMachineConfigClass)) {
				Class<?>[] generics = ResolvableType.forClass(stateMachineConfigClass).as(StateMachineConfigurer.class).resolveGenerics();
				if (generics.length == 2) {
					BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(StateMachineImpl.class);
					beanDefinitionBuilder.addConstructorArgValue(stateMachineConfigClass);
					RootBeanDefinition beanDefinition = (RootBeanDefinition) beanDefinitionBuilder.getBeanDefinition();
					beanDefinition.setTargetType(ResolvableType.forClassWithGenerics(StateMachine.class, generics[0], generics[1]));

					String beanName = DefaultBeanNameGenerator.INSTANCE.generateBeanName(beanDefinitionBuilder.getBeanDefinition(), registry);
					registry.registerBeanDefinition(beanName, beanDefinition);
				}
			}
		} catch (Exception e) {
			log.error("StateMachineBeanDefinitionRegistrar error,message: {}", e.getMessage(), e);
		}
	}
}
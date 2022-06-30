package com.tapdata.tm.statemachine.annotation;

import com.tapdata.tm.statemachine.configuration.StateMachineBeanDefinitionRegistrar;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Configuration
@Import(StateMachineBeanDefinitionRegistrar.class)
public @interface EnableStateMachine {

	boolean enabled() default true;
}

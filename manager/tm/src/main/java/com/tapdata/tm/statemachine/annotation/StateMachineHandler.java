/**
 * @title: StateMachineHandler
 * @description:
 * @author lk
 * @date 2021/8/11
 */
package com.tapdata.tm.statemachine.annotation;

import com.tapdata.tm.statemachine.enums.Transitions;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(StateMachineHandlers.class)
public @interface StateMachineHandler {

	Transitions transitions();
}

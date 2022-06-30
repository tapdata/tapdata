/**
 * @title: AbstractStateMachine
 * @description:
 * @author lk
 * @date 2021/11/12
 */
package com.tapdata.tm.statemachine;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.StateTrigger;
import com.tapdata.tm.utils.SpringContextHelper;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;

@Slf4j
public class StateMachineImpl<S, E> implements StateMachine<S, E> {

	private final StateMachineExecutor<S, E> executor = new StateMachineExecutor<>();

	private final Class<? extends StateMachineConfigurer<S, E>> stateMachineConfigurerClass;

	private Class<? extends StateContext<S, E>> stateContextClass;

	private Class<? extends StateTrigger<S, E>> stateTriggerClass;

	public StateMachineImpl(Class<? extends StateMachineConfigurer<S, E>> stateMachineConfigurerClass) {
		this.stateMachineConfigurerClass = stateMachineConfigurerClass;
	}

	public void initStateTransition(){
		if (stateMachineConfigurerClass != null){
			StateMachineConfigurer<S, E> stateMachineConfigurer = SpringContextHelper.getBean(stateMachineConfigurerClass);
			executor.initStateTransition(stateMachineConfigurer, this);
			this.stateContextClass = stateMachineConfigurer.getContextClass();
			this.stateTriggerClass = stateMachineConfigurer.getTriggeClass();
		}
	}


	public StateMachineResult handleEvent(StateTrigger<S, E> trigger){
		return executor.handleEvent(buildContext(trigger));
	}

	private StateContext<S, E> buildContext(StateTrigger<S, E> trigger){
		try {
			if (stateContextClass != null){
				Class<?> triggerClass = getTriggerClass();
				if (triggerClass == null || triggerClass != trigger.getClass()){
					StateContext<S, E> context = new StateContext<>(trigger);
					if (Arrays.stream(stateContextClass.getConstructors()).anyMatch(constructor -> constructor.getParameterCount() == 0)) {
						StateContext<S, E> stateContext = stateContextClass.newInstance();
						BeanUtils.copyProperties(context, stateContext);
						return stateContext;
					}
					return context;
				}
//				Object newInstance = triggerClass.newInstance();
//				BeanUtils.copyProperties(trigger, newInstance);
				return stateContextClass.getDeclaredConstructor(triggerClass).newInstance(trigger);
			}else {
				return new StateContext<>(trigger);
			}
		} catch (Exception e) {
			log.error("Build stateContext failed,message: {}", e.getMessage(), e);
			throw new BizException("Build stateContext failed,message:" + e.getMessage());
		}
	}

	private Class<?> getTriggerClass(){
		Constructor<?>[] declaredConstructors = stateContextClass.getDeclaredConstructors();
		return Arrays.stream(declaredConstructors)
				.map(Constructor::getParameterTypes)
				.filter(parameterTypes -> parameterTypes.length == 1)
				.filter(parameterTypes -> (stateTriggerClass != null && parameterTypes[0] == stateTriggerClass)
						|| (StateTrigger.class.isAssignableFrom(parameterTypes[0])))
				.findFirst()
				.map(parameterTypes -> parameterTypes[0])
				.orElse(null);
	}

}

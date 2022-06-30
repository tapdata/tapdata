/**
 * @title: AbsStateMachineConfigurer
 * @description:
 * @author lk
 * @date 2021/11/12
 */
package com.tapdata.tm.statemachine.configuration;

import com.tapdata.tm.statemachine.Processor;
import com.tapdata.tm.statemachine.StateMachine;
import com.tapdata.tm.statemachine.StateMachineConfigurer;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.StateTrigger;
import com.tapdata.tm.statemachine.model.Transition;
import com.tapdata.tm.statemachine.utils.StateMachineProcessorManager;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

public abstract class AbstractStateMachineConfigurer<S, E> implements StateMachineConfigurer<S, E> {

	private StateMachineBuilder<S, E> builder;

	private DefaultListableBeanFactory beanFactory;

	private StateMachine<S, E> stateMachine;

	public void build(Map<S, Map<E, Transition<S, E>>> stateTransitionMap, StateMachine<S, E> stateMachine){
		if (builder == null){
			builder = new StateMachineBuilder<>();
		}
		this.stateMachine = stateMachine;
		configure(builder);
		configureAction(stateTransitionMap);
	}

	public void configureAction(Map<S, Map<E, Transition<S, E>>> stateTransitionMap) {
		if (builder != null && stateTransitionMap != null){
			for (Transition<S, E> transition : builder.getTransitions()) {
				if (transition.getSource() == null || transition.getTarget() == null){
					continue;
				}
				Processor processor = StateMachineProcessorManager.getProcessor(transition.getSource(), transition.getEvent());
				if (processor != null){
					processor.configure(transition);
				}else {
					transition.action(StateMachineProcessorManager.getAction(transition, getContextClass(), stateMachine));
				}
				if (transition.getAction() == null){
					transition.action(commonAction());
				}
				if (!stateTransitionMap.containsKey(transition.getSource())){
					stateTransitionMap.put(transition.getSource(), new HashMap<>());
				}
				stateTransitionMap.get(transition.getSource()).put(transition.getEvent(), transition);
			}
		}
	}

	public Function<StateContext<S, E>, StateMachineResult> commonAction(){
		return null;
	};

	@Override
	public Class<? extends StateContext<S, E>> getContextClass() {
		return null;
	}

	@Override
	public Class<? extends StateTrigger<S, E>> getTriggeClass() {
		return null;
	}
}

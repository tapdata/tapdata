/**
 * @title: StateMachineConfigurer
 * @description:
 * @author lk
 * @date 2021/11/14
 */
package com.tapdata.tm.statemachine;

import com.tapdata.tm.statemachine.configuration.StateMachineBuilder;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateTrigger;
import com.tapdata.tm.statemachine.model.Transition;
import java.util.Map;

public interface StateMachineConfigurer<S, E> {

	void build(Map<S, Map<E, Transition<S, E>>> stateTransitionMap, StateMachine<S, E> stateMachine);

	void configure(StateMachineBuilder<S, E> builder);

	void configureAction(Map<S, Map<E, Transition<S, E>>> stateTransitionMap);

	Class<? extends StateContext<S, E>> getContextClass();

	Class<? extends StateTrigger<S, E>> getTriggeClass();
}

/**
 * @title: StateMachineBuilder
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.tm.statemachine.configuration;

import com.tapdata.tm.statemachine.model.Transition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

public class StateMachineBuilder<S, E> {

	private List<Transition<S, E>> transitions;

	List<Transition<S, E>> getTransitions() {
		if (CollectionUtils.isEmpty(transitions)){
			return Collections.emptyList();
		}
		return transitions;
	}

	public Transition<S, E> add(Transition<S, E> transition){
		if (CollectionUtils.isEmpty(transitions)){
			transitions = new ArrayList<>();
		}
		transition.setBuilder(this);
		transitions.add(transition);
		return transition;
	}

	public Transition<S, E> transition(){
		return add(new Transition<S, E>(null, null, null));
	}

	public StateMachineBuilder<S, E> transition(S source, S target, E event){
		add(new Transition<S, E>(source, target, event));
		return this;
	}
}

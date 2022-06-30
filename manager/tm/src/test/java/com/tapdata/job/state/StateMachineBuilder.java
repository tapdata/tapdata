/**
 * @title: StateMachineBuilder
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

public class StateMachineBuilder {

	private List<Transition> transitions;

	public List<Transition> getTransitions() {
		return transitions;
	}

	public void setTransitions(List<Transition> transitions) {
		this.transitions = transitions;
	}

	public Transition add(Transition transition){
		if (CollectionUtils.isEmpty(transitions)){
			transitions = new ArrayList<>();
		}
		transition.setBuilder(this);
		transitions.add(transition);
		return transition;
	}

	public Transition transition(){
		return add(Transition.build());
	}
}

/**
 * @title: StateMachineExecutor
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;

public class StateMachineExecutor {

	private Map<State, Map<Event, Transition>> stateTransitionMap = new HashMap<>();


	public StateMachineExecutor(){
		StateMachineBuilder build = new StateMachineConfigurer().build();
		if (build != null){
			List<Transition> transitions = build.getTransitions();
			for (Transition transition : transitions) {
				if (!stateTransitionMap.containsKey(transition.getSource())){
					stateTransitionMap.put(transition.getSource(), new HashMap<>());
				}
				stateTransitionMap.get(transition.getSource()).put(transition.getEvent(), transition);
			}
		}
	}

	public void handleEvent(StateMachineContext stateMachineContext){
		if (stateTransitionMap.containsKey(stateMachineContext.getSource())){
			Map<Event, Transition> transitionMap = this.stateTransitionMap.get(stateMachineContext.getSource());
			if (transitionMap.containsKey(stateMachineContext.getEvent())){
				Transition transition = transitionMap.get(stateMachineContext.getEvent());
				stateMachineContext.setTarget(transition.getTarget());
				List<Gurad> gurads = transition.getGurads();
				if (CollectionUtils.isNotEmpty(gurads)){
					boolean gurad = gurad(gurads, stateMachineContext);
					if (!gurad){
						System.out.println("gurad failed, exit");
						return;
					}
				}
				//todo update job status
				System.out.println("success,change status [" + stateMachineContext.getSource() + "] to [" + stateMachineContext.getTarget() + "],event: " + stateMachineContext.getEvent());
				action(transition.getActions(), stateMachineContext);
			}else {
				System.out.println("error,target state does not exist,can not change status");
			}
		}else {
			System.out.println("error,source state does not exist");
		}
		System.out.println("Handle event end");
	}

	private boolean gurad(List<Gurad> gurads,StateMachineContext context){
		boolean result = true;
		for (Gurad gurad : gurads) {
			if (!gurad.evaluate(context)) {
				System.out.println("State machine gurad failed...");
				result = false;
				break;
			}
		}

		return result;
	}

	private void action(List<Action> actions, StateMachineContext context){
		if (CollectionUtils.isNotEmpty(actions)){
			for (Action action : actions) {
//				CompletableFuture.runAsync(()->action.execute(context));
				action.execute(context);
			}
		}
	}
}

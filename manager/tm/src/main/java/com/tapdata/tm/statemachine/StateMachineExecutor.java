/**
 * @title: StateMachineExecutor
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.tm.statemachine;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.statemachine.enums.TaskState;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.Transition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

@Slf4j
public class StateMachineExecutor<S, E> {

	private final Map<S, Map<E, Transition<S, E>>> stateTransitionMap = new HashMap<>();

	void initStateTransition(StateMachineConfigurer<S, E> stateMachineConfigurer, StateMachine<S, E> stateMachine){
		if (stateMachineConfigurer != null){
			stateMachineConfigurer.build(stateTransitionMap, stateMachine);
		}
	}

	public StateMachineResult handleEvent(StateContext<S, E> context){
		if (MapUtils.isEmpty(stateTransitionMap)){
//			throw new StateException(context, "Transition is empty");
			throw new BizException("IllegalArgument", "transition");
		}
		if (stateTransitionMap.containsKey(context.getSource())){
			StateMachineResult result = null;
			Map<E, Transition<S, E>> transitionMap = stateTransitionMap.get(context.getSource());
			if (transitionMap.containsKey(context.getEvent())){
				Transition<S, E> transition = transitionMap.get(context.getEvent());
				context.setTarget(transition.getTarget());
				log.info("The status changes from {} to {} start,context: {}", context.getSource(), context.getTarget(), JsonUtil.toJson(context));
				List<Function<StateContext<S, E>, Boolean>> guards = transition.getGuards();
				if (CollectionUtils.isNotEmpty(guards)){
					boolean guard = guard(guards, context);
					if (!guard){
						log.warn("Guard failed,context: {}", JsonUtil.toJson(context));
//						throw new StateException(context,"guard failed");
						throw new BizException("Transition.Not.Supported");
					}
				}
				if (transition.getAction() != null){
					result = transition.getAction().apply(context);
					result.setBefore(((TaskState)transition.getSource()).getName());
					result.setAfter(((TaskState)transition.getTarget()).getName());
				}
				if (context.getNeedPostProcessor() != null && context.getNeedPostProcessor()){
					log.info("The status changes from {} to {} successfully,context: {}", context.getSource(), context.getTarget(), JsonUtil.toJson(context));
					afterAction(transition.getAfterActions(), context);
				}
				log.info("The status changes from {} to {} end,context: {}", context.getSource(), context.getTarget(), JsonUtil.toJson(context));
				return result == null ? StateMachineResult.ok() : result;
			}else {
				log.warn("Failed to change status from {},This event is not supported in the state and the state cannot be changed,context: {}", context.getSource(), JsonUtil.toJson(context));
//				throw new StateException(context, "This event is not supported in the state");
				throw new BizException("Transition.Not.Supported");
			}
		}else {
			log.warn("Failed to change status from {},transition does not exist,context: {}", context.getSource(), JsonUtil.toJson(context));
//			throw new StateException(context, "Transition does not exist");
			throw new BizException("Transition.Not.Found");
		}
	}

	private boolean guard(List<Function<StateContext<S, E>, Boolean>> guards, StateContext<S, E> context){
		return guards.stream().allMatch(guard -> guard.apply(context));
	}

	private void afterAction(List<Consumer<StateContext<S, E>>> afterActions, StateContext<S, E> context){
		if (CollectionUtils.isNotEmpty(afterActions)){
			afterActions.forEach(afterAction -> afterAction.accept(context));
		}
	}

	public Map<S, Map<E, Transition<S, E>>> getStateTransitionMap(){
		return stateTransitionMap;
	}
}

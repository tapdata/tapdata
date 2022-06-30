/**
 * @title: Transition
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.tm.statemachine.model;

import com.tapdata.tm.statemachine.configuration.StateMachineBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.collections4.CollectionUtils;

public class Transition<S, E> {

	private S source;

	private S target;

	private E event;

	private List<Function<StateContext<S, E>, Boolean>> guards;

	private Function<StateContext<S, E>, StateMachineResult> action;

	private List<Consumer<StateContext<S, E>>> afterActions;

	private StateMachineBuilder<S, E> builder;

	public Transition(S source, S target, E event) {
		this.source = source;
		this.target = target;
		this.event = event;
	}

	public void setBuilder(StateMachineBuilder<S, E> builder) {
		this.builder = builder;
	}

	private StateMachineBuilder<S, E> getBuilder() {
		return builder;
	}

	public StateMachineBuilder<S, E> and(){
		return getBuilder();
	}

	public S getSource() {
		return source;
	}

	public S getTarget() {
		return target;
	}

	public E getEvent() {
		return event;
	}

	public List<Function<StateContext<S, E>, Boolean>> getGuards() {
		return guards;
	}

	public Function<StateContext<S, E>, StateMachineResult> getAction() {
		return action;
	}

	public List<Consumer<StateContext<S, E>>> getAfterActions() {
		return afterActions;
	}

	private void setSource(S source) {
		this.source = source;
	}

	private void setTarget(S target) {
		this.target = target;
	}

	private void setEvent(E event) {
		this.event = event;
	}

	public void setGuards(List<Function<StateContext<S, E>, Boolean>> guards) {
		this.guards = guards;
	}

	public void setAction(Function<StateContext<S, E>, StateMachineResult> action) {
		this.action = action;
	}

	public void setAfterActions(List<Consumer<StateContext<S, E>>> afterActions) {
		this.afterActions = afterActions;
	}

	public Transition<S, E> source(S state){
		setSource(state);
		return this;
	}


	public Transition<S, E> target(S state){
		setTarget(state);
		return this;
	}

	public Transition<S, E> event(E event){
		setEvent(event);
		return this;
	}

	public Transition<S, E> guard(Function<StateContext<S, E>, Boolean> guard){
		if (CollectionUtils.isEmpty(this.guards)){
			this.guards = new ArrayList<>();
		}
		this.guards.add(guard);
		return this;
	}

	public Transition<S, E> action(Function<StateContext<S, E>, StateMachineResult> action){
		setAction(action);
		return this;
	}
	public Transition<S, E> afterAction(Consumer<StateContext<S, E>> action){
		if (CollectionUtils.isEmpty(this.afterActions)){
			this.afterActions = new ArrayList<>();
		}
		this.afterActions.add(action);
		return this;
	}


}

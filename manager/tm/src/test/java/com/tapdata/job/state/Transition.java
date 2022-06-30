/**
 * @title: Transition
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

public class Transition {

	private State source;

	private State target;

	private Event event;

	private List<Gurad> gurads;

	private List<Action> actions;

	private StateMachineBuilder builder;

	public static Transition build(){
		return new Transition(null, null, null);
	}

	public Transition(State source, State target, Event event) {
		this.source = source;
		this.target = target;
		this.event = event;
	}

	public void setBuilder(StateMachineBuilder builder) {
		this.builder = builder;
	}

	private StateMachineBuilder getBuilder() {
		return builder;
	}

	public StateMachineBuilder and(){
		return getBuilder();
	}

	public State getSource() {
		return source;
	}

	public State getTarget() {
		return target;
	}

	public Event getEvent() {
		return event;
	}

	public List<Gurad> getGurads() {
		return gurads;
	}

	public List<Action> getActions() {
		return actions;
	}

	private void setSource(State source) {
		this.source = source;
	}

	private void setTarget(State target) {
		this.target = target;
	}

	private void setEvent(Event event) {
		this.event = event;
	}

	private void setGurads(List<Gurad> gurads) {
		this.gurads = gurads;
	}

	private void setActions(List<Action> actions) {
		this.actions = actions;
	}

	public Transition source(State state){
		setSource(state);
		return this;
	}

	public Transition target(State state){
		setTarget(state);
		return this;
	}

	public Transition event(Event event){
		setEvent(event);
		return this;
	}

	public Transition gurad(Gurad gurad){
		if (CollectionUtils.isEmpty(this.gurads)){
			this.gurads = new ArrayList<>();
		}
		this.gurads.add(gurad);
		return this;
	}

	public Transition action(Action action){
		if (CollectionUtils.isEmpty(this.actions)){
			this.actions = new ArrayList<>();
		}
		this.actions.add(action);
		return this;
	}

}

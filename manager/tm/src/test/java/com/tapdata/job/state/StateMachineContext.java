/**
 * @title: StateContext
 * @description:
 * @author lk
 * @date 2021/7/29
 */
package com.tapdata.job.state;

import org.springframework.data.mongodb.core.MongoTemplate;

public class StateMachineContext {

	private State source;

	private State target;

	private Event event;

	private Transition transition;

	private MongoTemplate mongoTemplate;

	public Transition getTransition() {
		return transition;
	}

	public MongoTemplate getMongoTemplate() {
		return mongoTemplate;
	}

	public void setTransition(Transition transition) {
		this.transition = transition;
	}

	public void setMongoTemplate(MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
	}

	public State getSource() {
		return source;
	}

	public State getTarget() {
		return target;
	}

	public void setSource(State source) {
		this.source = source;
	}

	public void setTarget(State target) {
		this.target = target;
	}

	public Event getEvent() {
		return event;
	}

	public void setEvent(Event event) {
		this.event = event;
	}
}

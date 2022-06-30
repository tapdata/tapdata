package com.tapdata.tm.statemachine;

import com.tapdata.tm.statemachine.model.StateContext;

@FunctionalInterface
public interface Guard {

	boolean evaluate(StateContext context);
}
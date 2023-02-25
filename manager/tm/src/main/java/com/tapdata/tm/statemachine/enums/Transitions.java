/**
 * @title: State
 * @description:
 * @author lk
 * @date 2021/11/15
 */
package com.tapdata.tm.statemachine.enums;

public enum Transitions {

	/**
	 * source: edit/stopped/error
	 * target: scheduled
	 **/
	DATAFLOW_START(DataFlowEvent.START, new DataFlowState[]{DataFlowState.EDIT,DataFlowState.STOPPED,DataFlowState.ERROR}),
	/**
	 * source: running
	 * target: scheduling
	 **/
	DATAFLOW_OVERTIME(DataFlowEvent.OVERTIME, new DataFlowState[]{DataFlowState.RUNNING}),

	/**
	 * source: edit/stopped/error
	 * target: scheduling
	 **/
	SUBTASK_START(DataFlowEvent.START, new TaskState[]{TaskState.WAIT_START, TaskState.STOPPED, TaskState.ERROR, TaskState.DONE, TaskState.SCHEDULING_FAILED}),
	/**
	 * source: scheduling
	 * target: wait_run
	 **/
	SUBTASK_SCHEDULE_SUCEESS(DataFlowEvent.SCHEDULE_SUCCESS, new TaskState[]{TaskState.SCHEDULING}),
	/**
	 * source: wait_run
	 * target: scheduling
	 **/
	SUBTASK_OVERTIME(DataFlowEvent.OVERTIME, new TaskState[]{TaskState.WAITING_RUN}),
	;

	private Enum event;
	
	private Enum[] sources;

	Transitions(Enum event, Enum[] sources) {
		this.event = event;
		this.sources = sources;
	}

	public Enum getEvent() {
		return event;
	}

	public Enum[] getSources() {
		return sources;
	}
}

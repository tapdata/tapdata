/**
 * @title: State
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.tm.statemachine.enums;

import static com.tapdata.tm.statemachine.constant.StateMachineConstant.*;
import java.util.HashMap;
import java.util.Map;

public enum DataFlowState {

	/**
	 * 编辑中
	 **/
	EDIT(DATAFLOW_STATUS_EDIT),
	/**
	 * 调度中
	 **/
	SCHEDULING(DATAFLOW_STATUS_SCHEDULING),
	/**
	 * 调度失败
	 **/
	SCHEDULING_FAILED(DATAFLOW_STATUS_SCHEDULING_FAILED),
	/**
	 * 待运行
	 **/
	WAITING_RUN(DATAFLOW_STATUS_WAITING_RUN),
	/**
	 * 运行中
	 **/
	RUNNING(DATAFLOW_STATUS_RUNNING),
	/**
	 * 停止中
	 **/
	STOPPING(DATAFLOW_STATUS_STOPPING),
	/**
	 * 强制停止中
	 **/
	FORCE_STOPPING(DATAFLOW_STATUS_FORCE_STOPPING),
	/**
	 * 错误
	 **/
	ERROR(DATAFLOW_STATUS_ERROR),
	/**
	 * 已停止
	 **/
	STOPPED(DATAFLOW_STATUS_STOPPED),
	/**
	 * 已完成
	 **/
	DONE(DATAFLOW_STATUS_DONE);

	private String name;

	DataFlowState(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	private static final Map<String, DataFlowState> map = new HashMap<>();

	static {
		for (DataFlowState value : DataFlowState.values()) {
			map.put(value.getName(),value);
		}
	}

	public static DataFlowState getState(String name){
		return map.get(name);
	}

	@Override
	public String toString() {
		return getName();
	}
}

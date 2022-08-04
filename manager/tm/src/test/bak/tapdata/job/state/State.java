/**
 * @title: State
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state;

import static com.tapdata.job.JobStatusConstant.*;
import java.util.HashMap;
import java.util.Map;

public enum State {

	/**
	 * 编辑中
	 **/
	EDIT(JOB_STATUS_EDIT),
	/**
	 * 调度中
	 **/
	SCHEDULING(JOB_STATUS_SCHEDULING),
	/**
	 * 调度失败
	 **/
	SCHEDULING_FAILED(JOB_STATUS_SCHEDULING_FAILED),
	/**
	 * 待运行
	 **/
	WAITING_RUN(JOB_STATUS_WAITING_RUN),
	/**
	 * 运行中
	 **/
	RUNNING(JOB_STATUS_RUNNING),
	/**
	 * 停止中
	 **/
	STOPPING(JOB_STATUS_STOPPING),
	/**
	 * 错误
	 **/
	ERROR(JOB_STATUS_ERROR),
	/**
	 * 已停止
	 **/
	STOPPED(JOB_STATUS_STOPPED),
	/**
	 * 已完成
	 **/
	DONE(JOB_STATUS_DONE);

	private String name;

	State(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	private static final Map<String, State> map = new HashMap<>();

	static {
		for (State value : State.values()) {
			map.put(value.getName(),value);
		}
	}

	public static State getState(String name){
		return map.get(name);
	}
}

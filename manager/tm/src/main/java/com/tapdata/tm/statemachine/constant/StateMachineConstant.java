/**
 * @title: Constant
 * @description:
 * @author lk
 * @date 2021/8/11
 */
package com.tapdata.tm.statemachine.constant;

public class StateMachineConstant {

	/**
	 * 编辑中
	 **/
	public static final String DATAFLOW_STATUS_EDIT = "edit";
	/**
	 * 调度中
	 **/
	public static final String DATAFLOW_STATUS_SCHEDULING = "scheduled";
	/**
	 * 调度失败
	 **/
	public static final String DATAFLOW_STATUS_SCHEDULING_FAILED = "schedule_failed";
	/**
	 * 待运行
	 **/
	public static final String DATAFLOW_STATUS_WAITING_RUN = "wait_run";
	/**
	 * 运行中
	 **/
	public static final String DATAFLOW_STATUS_RUNNING = "running";
	/**
	 * 停止中
	 **/
	public static final String DATAFLOW_STATUS_STOPPING = "stopping";
	/**
	 * 强制停止中
	 **/
	public static final String DATAFLOW_STATUS_FORCE_STOPPING = "force stopping";
	/**
	 * 错误
	 **/
	public static final String DATAFLOW_STATUS_ERROR = "error";
	/**
	 * 已停止
	 **/
	public static final String DATAFLOW_STATUS_STOPPED = "paused";
	/**
	 * 已完成
	 **/
	public static final String DATAFLOW_STATUS_DONE = "complete";


	/**
	 * 启动任务
	 **/
	public static final String DATAFLOW_EVENT_START = "start";
	/**
	 * 停止任务
	 **/
	public static final String DATAFLOW_EVENT_STOP = "stop";
	/**
	 * 强制停止任务
	 **/
	public static final String DATAFLOW_EVENT_FORCE_STOP = "force_stop";
	/**
	 * 调度失败
	 **/
	public static final String DATAFLOW_EVENT_SCHEDULE_FAILED = "schedule_failed";
	/**
	 * 调度成功
	 **/
	public static final String DATAFLOW_EVENT_SCHEDULE_SUCCESS = "schedule_success";
	/**
	 * 编辑任务
	 **/
	public static final String DATAFLOW_EVENT_EDIT = "edit";
	/**
	 * 重启任务
	 **/
	public static final String DATAFLOW_EVENT_SCHEDULE_RESTART = "schedule_restart";
	/**
	 * 任务运行中
	 **/
	public static final String DATAFLOW_EVENT_RUNNING = "running";
	/**
	 * 引擎心跳超时
	 **/
	public static final String DATAFLOW_EVENT_OVERTIME = "overtime";
	/**
	 * 引擎正常退出
	 **/
	public static final String DATAFLOW_EVENT_EXIT = "exit";
	/**
	 * 任务执行完成
	 **/
	public static final String DATAFLOW_EVENT_COMPLETED = "completed";
	/**
	 * 任务停止完成
	 **/
	public static final String DATAFLOW_EVENT_STOPPED = "stopped";
	/**
	 * 任务执行错误
	 **/
	public static final String DATAFLOW_EVENT_ERROR = "error";
}

/**
 * @title: JobStatusConstant
 * @description:
 * @author lk
 * @date 2021/7/19
 */
package com.tapdata.job;

public class JobStatusConstant {

	/**
	 * 编辑中
	 **/
	public static final String JOB_STATUS_EDIT = "EDIT";
	/**
	 * 调度中
	 **/
	public static final String JOB_STATUS_SCHEDULING = "SCHEDULING";
	/**
	 * 调度失败
	 **/
	public static final String JOB_STATUS_SCHEDULING_FAILED = "SCHEDULING_FAILED";
	/**
	 * 待运行
	 **/
	public static final String JOB_STATUS_WAITING_RUN = "WAITING_RUN";
	/**
	 * 运行中
	 **/
	public static final String JOB_STATUS_RUNNING = "RUNNING";
	/**
	 * 停止中
	 **/
	public static final String JOB_STATUS_STOPPING = "STOPPING";
	/**
	 * 错误
	 **/
	public static final String JOB_STATUS_ERROR = "ERROR";
	/**
	 * 已停止
	 **/
	public static final String JOB_STATUS_STOPPED = "STOPPED";
	/**
	 * 已完成
	 **/
	public static final String JOB_STATUS_DONE = "DONE";

}

/**
 * @title: Event
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state;

public enum Event {

	START, //启动任务
	STOPPING, //停止任务
	FORCE_STOP, //强制停止任务
	SCHEDULE_FAILED,//调度失败
	SCHEDULE_SUCCESS, //调度成功
	EDIT, //编辑任务
	SCHEDULE_RESTART, //重启任务
	RUNNING, //任务运行中
	OVERTIME, //引擎心跳超时
	EXIT, //引擎正常退出
	COMPLETED, //任务执行完成
	STOPPED, //任务执行完成
	ERROR //任务执行错误
}

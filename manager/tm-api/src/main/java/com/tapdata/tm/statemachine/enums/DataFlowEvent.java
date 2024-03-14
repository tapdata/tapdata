/**
 * @title: Event
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.tm.statemachine.enums;

import static com.tapdata.tm.statemachine.constant.StateMachineConstant.*;
import java.util.HashMap;
import java.util.Map;

public enum DataFlowEvent {

	START(DATAFLOW_EVENT_START), //启动任务
	STOP(DATAFLOW_EVENT_STOP), //停止任务
	FORCE_STOP(DATAFLOW_EVENT_FORCE_STOP), //强制停止任务
	SCHEDULE_FAILED(DATAFLOW_EVENT_SCHEDULE_FAILED),//调度失败
	SCHEDULE_SUCCESS(DATAFLOW_EVENT_SCHEDULE_SUCCESS), //调度成功
	EDIT(DATAFLOW_EVENT_EDIT), //编辑任务
	SCHEDULE_RESTART(DATAFLOW_EVENT_SCHEDULE_RESTART), //重启任务
	RUNNING(DATAFLOW_EVENT_RUNNING), //任务运行中
	OVERTIME(DATAFLOW_EVENT_OVERTIME), //超时(接管、心跳、停止)
	EXIT(DATAFLOW_EVENT_EXIT), //引擎正常退出
	COMPLETED(DATAFLOW_EVENT_COMPLETED), //任务执行完成
	STOPPED(DATAFLOW_EVENT_STOPPED), //任务停止完成
	ERROR(DATAFLOW_EVENT_ERROR), //任务执行错误
	CONFIRM(DATAFLOW_EVENT_CONFIRM), //任务保存
	RENEW(DATAFLOW_EVENT_RENEW), //任务保存
	DELETE(DATAFLOW_EVENT_DELETE), //任务保存
	RENEW_DEL_FAILED(DATAFLOW_EVENT_RENEW_DEL_FAILED), //任务保存
	RENEW_DEL_SUCCESS(DATAFLOW_EVENT_RENEW_DEL_SUCCESS), //任务保存

	;

	private String name;

	DataFlowEvent(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}


	private static final Map<String, DataFlowEvent> map = new HashMap<>();

	static {
		for (DataFlowEvent value : DataFlowEvent.values()) {
			map.put(value.getName(),value);
		}
	}

	public static DataFlowEvent getEvent(String name){
		return map.get(name);
	}
}

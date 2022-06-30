/**
 * @title: JobStatus
 * @description:
 * @author lk
 * @date 2021/7/19
 */
package com.tapdata.job;

import static com.tapdata.job.JobStatusConstant.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public enum JobStatus {

	/**
	 * 编辑中
	 **/
	EDIT(JOB_STATUS_EDIT, Arrays.asList(JOB_STATUS_SCHEDULING_FAILED,JOB_STATUS_ERROR,JOB_STATUS_STOPPED,JOB_STATUS_DONE)),
	/**
	 * 调度中
	 **/
	SCHEDULING(JOB_STATUS_SCHEDULING, Arrays.asList(JOB_STATUS_EDIT,JOB_STATUS_SCHEDULING_FAILED,JOB_STATUS_WAITING_RUN,JOB_STATUS_ERROR,JOB_STATUS_STOPPED,JOB_STATUS_DONE)),
	/**
	 * 调度失败
	 **/
	SCHEDULING_FAILED(JOB_STATUS_SCHEDULING_FAILED, Collections.singletonList(JOB_STATUS_SCHEDULING)),
	/**
	 * 待运行
	 **/
	WAITING_RUN(JOB_STATUS_WAITING_RUN, Arrays.asList(JOB_STATUS_SCHEDULING,JOB_STATUS_RUNNING)),
	/**
	 * 运行中
	 **/
	RUNNING(JOB_STATUS_RUNNING, Collections.singletonList(JOB_STATUS_WAITING_RUN)),
	/**
	 * 停止中
	 **/
	STOPING(JOB_STATUS_STOPPING, Arrays.asList(JOB_STATUS_WAITING_RUN,JOB_STATUS_RUNNING)),
	/**
	 * 错误
	 **/
	ERROR(JOB_STATUS_ERROR, Arrays.asList(JOB_STATUS_RUNNING,JOB_STATUS_STOPPING)),
	/**
	 * 已停止
	 **/
	STOPPED(JOB_STATUS_STOPPED, Collections.singletonList(JOB_STATUS_STOPPING)),
	/**
	 * 已完成
	 **/
	DONE(JOB_STATUS_DONE, Arrays.asList(JOB_STATUS_RUNNING,JOB_STATUS_STOPPING));

	private String target;

	private List<String> source;

	JobStatus(String target, List<String> source) {
		this.target = target;
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public List<String> getSource() {
		return source;
	}

	private static final Map<String, JobStatus> map = new HashMap<>();

	static {
		for (JobStatus value : JobStatus.values()) {
			map.put(value.getTarget(),value);
		}
	}

	public static JobStatus getStatus(String target){
		return map.get(target);
	}

	public static Set<String> getTargetstatus(){
		return map.keySet();
	}

}

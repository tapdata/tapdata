package io.tapdata.flow.engine.V2.common;

import com.hazelcast.jet.core.JobStatus;
import com.tapdata.tm.commons.task.dto.TaskDto;

/**
 * task状态与hazelcast job status映射枚举
 *
 * @author jackin
 * @date 2021/3/9 6:29 PM
 **/
public enum HazelcastStatusMappingEnum {

	NOT_RUNNING(JobStatus.NOT_RUNNING, TaskDto.STATUS_SCHEDULING),

	STARTING(JobStatus.STARTING, TaskDto.STATUS_SCHEDULING),

	RUNNING(JobStatus.RUNNING, TaskDto.STATUS_RUNNING),

	SUSPENDED(JobStatus.SUSPENDED, TaskDto.STATUS_STOPPING),

	SUSPENDED_EXPORTING_SNAPSHOT(JobStatus.SUSPENDED_EXPORTING_SNAPSHOT, TaskDto.STATUS_STOPPING),

	COMPLETING(JobStatus.COMPLETING, TaskDto.STATUS_STOPPING),

	FAILED(JobStatus.FAILED, TaskDto.STATUS_ERROR),

	COMPLETED(JobStatus.COMPLETED, TaskDto.STATUS_COMPLETE),
	;

	/**
	 * task 状态
	 */
	private String subTaskStatus;

	/**
	 * jet任务状态
	 */
	private JobStatus jetJobStatus;

	HazelcastStatusMappingEnum(JobStatus jetJobStatus, String subTaskStatus) {
		this.subTaskStatus = subTaskStatus;
		this.jetJobStatus = jetJobStatus;
	}

	public static String fromJobStatus(JobStatus jobStatus) {
		for (HazelcastStatusMappingEnum jetJobStatusMappingEnum : values()) {
			if (jetJobStatusMappingEnum.jetJobStatus == jobStatus) {
				return jetJobStatusMappingEnum.subTaskStatus;
			}
		}

		return null;
	}
}

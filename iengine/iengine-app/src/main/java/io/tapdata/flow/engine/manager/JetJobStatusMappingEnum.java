package io.tapdata.flow.engine.manager;

import com.hazelcast.jet.core.JobStatus;
import io.tapdata.common.DataFlowStatus;

/**
 * data flow状态与jet job status映射枚举
 *
 * @author jackin
 * @date 2021/3/9 6:29 PM
 **/
public enum JetJobStatusMappingEnum {

	NOT_RUNNING(JobStatus.NOT_RUNNING, DataFlowStatus.SCHEDULED),

	STARTING(JobStatus.STARTING, DataFlowStatus.SCHEDULED),

	RUNNING(JobStatus.RUNNING, DataFlowStatus.RUNNING),

	SUSPENDED(JobStatus.SUSPENDED, DataFlowStatus.STOPPING),

	SUSPENDED_EXPORTING_SNAPSHOT(JobStatus.SUSPENDED_EXPORTING_SNAPSHOT, DataFlowStatus.STOPPING),

	COMPLETING(JobStatus.COMPLETING, DataFlowStatus.STOPPING),

	FAILED(JobStatus.FAILED, DataFlowStatus.ERROR),

	COMPLETED(JobStatus.COMPLETED, DataFlowStatus.PAUSED),
	;

	/**
	 * data flow 状态
	 */
	private DataFlowStatus dataFlowStatus;

	/**
	 * jet任务状态
	 */
	private JobStatus jetJobStatus;

	JetJobStatusMappingEnum(JobStatus jetJobStatus, DataFlowStatus dataFlowStatus) {
		this.dataFlowStatus = dataFlowStatus;
		this.jetJobStatus = jetJobStatus;
	}

	public static DataFlowStatus fromJobStatus(JobStatus jobStatus) {
		for (JetJobStatusMappingEnum jetJobStatusMappingEnum : values()) {
			if (jetJobStatusMappingEnum.jetJobStatus == jobStatus) {
				return jetJobStatusMappingEnum.dataFlowStatus;
			}
		}

		return null;
	}
}

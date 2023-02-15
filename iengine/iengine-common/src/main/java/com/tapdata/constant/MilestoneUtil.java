package com.tapdata.constant;

import io.tapdata.milestone.MilestoneService;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;

import java.util.Optional;

/**
 * @author samuel
 * @Description
 * @create 2020-12-28 18:32
 **/
public class MilestoneUtil {

	/**
	 * 更新里程碑工具方法
	 *
	 * @param milestoneService
	 * @param milestoneStage
	 * @param milestoneStatus
	 * @param errMsg
	 */
	@Deprecated // Implemented in io.tapdata.milestone.MilestoneAspectTask
	public static void updateMilestone(MilestoneService milestoneService, MilestoneStage milestoneStage, MilestoneStatus milestoneStatus,
									   String errMsg) {
//		if (milestoneStage == null || milestoneStatus == null) {
//			return;
//		}
//		Optional.ofNullable(milestoneService).ifPresent(m -> m.updateMilestoneStatusByCode(milestoneStage, milestoneStatus, errMsg));
	}

	@Deprecated // Implemented in io.tapdata.milestone.MilestoneAspectTask
	public static void updateMilestone(MilestoneService milestoneService, MilestoneStage milestoneStage, MilestoneStatus milestoneStatus) {
//		if (milestoneStage == null || milestoneStatus == null) {
//			return;
//		}
//		Optional.ofNullable(milestoneService).ifPresent(m -> m.updateMilestoneStatusByCode(milestoneStage, milestoneStatus));
	}

	public static String getEdgeKey(String sourceVertexName, String destVertexName) {
		return sourceVertexName + ":" + destVertexName;
	}
}

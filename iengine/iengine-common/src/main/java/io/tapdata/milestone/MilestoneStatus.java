package io.tapdata.milestone;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description 里程碑状态枚举类
 * @create 2020-12-23 17:09
 **/
public enum MilestoneStatus {

	WAITING("waiting"),
	RUNNING("running"),
	ERROR("error"),
	FINISH("finish"),
	;

	private String status;

	MilestoneStatus(String status) {
		this.status = status;
	}

	public String getStatus() {
		return status;
	}

	private static Map<String, MilestoneStatus> map = new HashMap<>();

	static {
		for (MilestoneStatus value : MilestoneStatus.values()) {
			map.put(value.getStatus(), value);
		}
	}

	public static MilestoneStatus fromStatus(String status) {
		return map.get(status);
	}
}

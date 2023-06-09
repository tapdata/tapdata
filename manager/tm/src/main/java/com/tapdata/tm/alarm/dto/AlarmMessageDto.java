package com.tapdata.tm.alarm.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AlarmMessageDto {

	private String taskId;
	private String name;
	private String agentId;
	private String userId;
	/**
	 * 是否发送消息
	 */
	private boolean systemOpen;
	private boolean emailOpen;
	private boolean smsOpen;
	private boolean wechatOpen;


}

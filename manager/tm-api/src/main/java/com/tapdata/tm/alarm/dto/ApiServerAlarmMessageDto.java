package com.tapdata.tm.alarm.dto;

import lombok.Data;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/17 14:21 Create
 * @description
 */
@Data
public class ApiServerAlarmMessageDto {
    String serverName;
    String serverId;

    String apiName;
    String apiId;

    String workerId;
    String workerName;

    String userId;
    /**
     * 是否发送消息
     */
    private boolean systemOpen;
    private boolean emailOpen;
    private boolean smsOpen;
    private boolean wechatOpen;
}

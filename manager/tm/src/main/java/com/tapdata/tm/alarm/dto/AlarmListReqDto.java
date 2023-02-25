package com.tapdata.tm.alarm.dto;

import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.message.constant.Level;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author jiuyetx
 * @date 2022/9/13
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlarmListReqDto {
    private AlarmStatusEnum status;
    private Level level;
    private String taskId;
    private String nodeId;
}

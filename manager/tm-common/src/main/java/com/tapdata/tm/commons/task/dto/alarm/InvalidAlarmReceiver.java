package com.tapdata.tm.commons.task.dto.alarm;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 解析时被跳过的接收对象。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InvalidAlarmReceiver {
    public static final String DELETED = "已删除";
    public static final String NO_EMAIL = "无邮箱";
    public static final String BAD_EMAIL = "格式非法";
    public static final String GROUP_MISSING = "组不存在";
    public static final String BAD_TYPE = "类型无效";

    private AlarmReceiverType type;
    private String id;
    private String reason;
}

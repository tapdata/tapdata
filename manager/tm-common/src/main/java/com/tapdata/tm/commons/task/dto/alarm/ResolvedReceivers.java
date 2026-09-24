package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 一次发送解析的结果。emails 为空且 mode 为 CUSTOM 时，不回落系统默认。
 */
@Data
public class ResolvedReceivers {
    private ReceiverResolveMode mode = ReceiverResolveMode.DEFAULT;
    private List<String> emails = new ArrayList<>();
    /** 小写邮箱到中文来源文案 */
    private Map<String, List<String>> sources = new LinkedHashMap<>();
    private List<InvalidAlarmReceiver> invalid = new ArrayList<>();
}

package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

@Data
public class BatchAlarmDetail {
    private String id;
    private String name;
    private String code;
    private String message;

    public static BatchAlarmDetail of(String id, String name, String code, String message) {
        BatchAlarmDetail detail = new BatchAlarmDetail();
        detail.setId(id);
        detail.setName(name);
        detail.setCode(code);
        detail.setMessage(message);
        return detail;
    }
}

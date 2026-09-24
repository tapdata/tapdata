package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class BatchAlarmResult {
    private int succeeded;
    private int failed;
    private int skipped;
    private List<BatchAlarmDetail> details = new ArrayList<>();

    public void add(BatchAlarmDetail detail) {
        details.add(detail);
        if (detail == null || detail.getCode() == null) {
            return;
        }
        switch (detail.getCode()) {
            case "ok" -> succeeded++;
            case "SYSTEM_DEFAULT", "insufficient.permissions" -> skipped++;
            default -> failed++;
        }
    }
}

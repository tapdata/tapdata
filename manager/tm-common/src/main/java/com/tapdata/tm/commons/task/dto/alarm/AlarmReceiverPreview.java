package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class AlarmReceiverPreview {
    private ReceiverResolveMode mode;
    private String status;
    private List<PreviewEmail> emails = new ArrayList<>();
    private List<InvalidAlarmReceiver> invalid = new ArrayList<>();

    @Data
    public static class PreviewEmail {
        private String email;
        private List<String> sources = new ArrayList<>();
    }
}

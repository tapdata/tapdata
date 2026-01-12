package com.tapdata.tm.group.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class GroupInfoRecordDetail {
    private String groupId;
    private String groupName;
    private String message;
    private List<RecordDetail> recordDetails = new ArrayList<>();

    @Data
    public static class RecordDetail {
        private String resourceName;
        private ResourceType resourceType;
        private RecordAction action;
        private String message;
    }

    public enum RecordAction {
        EXPORTED,
        IMPORTED,
        SKIPPED,
        REPLACED,
        ERRORED
    }
}

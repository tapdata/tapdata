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
        private String resourceId;
        private String resourceName;
        private ResourceType resourceType;
        private RecordAction action;
        private String message;
        private Boolean reset;
    }

    public enum RecordAction {
        // Resource import/export actions
        EXPORTED,
        IMPORTED,
        SKIPPED,
        NO_UPDATE,
        REPLACED,
        ERRORED,
        IMPORTING,

        // Git operation actions
        GIT_STEP_STARTED,      // Git operation step started
        GIT_STEP_SUCCESS,      // Git operation step completed successfully
        GIT_STEP_FAILED        // Git operation step failed
    }
}

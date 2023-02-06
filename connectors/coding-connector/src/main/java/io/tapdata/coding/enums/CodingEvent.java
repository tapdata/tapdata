package io.tapdata.coding.enums;

import io.tapdata.coding.utils.tool.Checker;

public enum CodingEvent {
    ITERATION_DELETED("ITERATION_DELETED", TapEventTypes.DELETED_EVENT, "Iterations"),
    ARTIFACTS_VERSION_CREATED("ARTIFACTS_VERSION_CREATED", TapEventTypes.CREATED_EVENT, ""),
    FILE_MOVED("FILE_MOVED", TapEventTypes.UPDATE_EVENT, ""),
    CI_JOB_CREATED("CI_JOB_CREATED", TapEventTypes.CREATED_EVENT, ""),
    ARTIFACTS_REPO_DELETED("ARTIFACTS_REPO_DELETED", TapEventTypes.DELETED_EVENT, ""),
    WIKI_CREATED("WIKI_CREATED", TapEventTypes.CREATED_EVENT, ""),
    GIT_MR_CREATED("GIT_MR_CREATED", TapEventTypes.CREATED_EVENT, ""),
    GIT_MR_CLOSED("GIT_MR_CLOSED", TapEventTypes.DELETED_EVENT, ""),
    MEMBER_CREATED("MEMBER_CREATED", TapEventTypes.CREATED_EVENT, "ProjectMembers"),
    GIT_MR_UPDATED("GIT_MR_UPDATED", TapEventTypes.UPDATE_EVENT, ""),
    CI_JOB_FINISHED("CI_JOB_FINISHED", TapEventTypes.UPDATE_EVENT, ""),
    WIKI_RESTORED_FROM_RECYCLE_BIN("WIKI_RESTORED_FROM_RECYCLE_BIN", TapEventTypes.UPDATE_EVENT, ""),
    ISSUE_UPDATED("ISSUE_UPDATED", TapEventTypes.UPDATE_EVENT, "Issues"),
    FILE_SHARE_UPDATED("FILE_SHARE_UPDATED", TapEventTypes.UPDATE_EVENT, ""),
    WIKI_MOVED_TO_RECYCLE_BIN("WIKI_MOVED_TO_RECYCLE_BIN", TapEventTypes.UPDATE_EVENT, ""),
    ISSUE_RELATIONSHIP_CHANGED("ISSUE_RELATIONSHIP_CHANGED", TapEventTypes.UPDATE_EVENT, "Issues"),
    ARTIFACTS_VERSION_DOWNLOAD_BLOCKED("ARTIFACTS_VERSION_DOWNLOAD_BLOCKED", TapEventTypes.UPDATE_EVENT, ""),
    WIKI_COPIED("WIKI_COPIED", TapEventTypes.UPDATE_EVENT, ""),
    ARTIFACTS_VERSION_DOWNLOADED("ARTIFACTS_VERSION_DOWNLOADED", TapEventTypes.UPDATE_EVENT, ""),
    ISSUE_ASSIGNEE_CHANGED("ISSUE_ASSIGNEE_CHANGED", TapEventTypes.UPDATE_EVENT, "Issues"),
    ARTIFACTS_VERSION_RELEASED("ARTIFACTS_VERSION_RELEASED", TapEventTypes.UPDATE_EVENT, ""),
    FILE_MOVED_TO_RECYCLE_BIN("FILE_MOVED_TO_RECYCLE_BIN", TapEventTypes.UPDATE_EVENT, ""),
    WIKI_SHARE_UPDATED("WIKI_SHARE_UPDATED", TapEventTypes.UPDATE_EVENT, ""),
    ARTIFACTS_VERSION_DOWNLOAD_FORBIDDEN("ARTIFACTS_VERSION_DOWNLOAD_FORBIDDEN", TapEventTypes.UPDATE_EVENT, ""),
    ITERATION_UPDATED("ITERATION_UPDATED", TapEventTypes.UPDATE_EVENT, "Iterations"),
    GIT_MR_NOTE("GIT_MR_NOTE", TapEventTypes.UPDATE_EVENT, ""),
    ISSUE_HOUR_RECORD_UPDATED("ISSUE_HOUR_RECORD_UPDATED", TapEventTypes.UPDATE_EVENT, "Issues"),
    FILE_CREATED("FILE_CREATED", TapEventTypes.CREATED_EVENT, ""),
    ARTIFACTS_REPO_UPDATED("ARTIFACTS_REPO_UPDATED", TapEventTypes.UPDATE_EVENT, ""),
    CI_JOB_DELETED("CI_JOB_DELETED", TapEventTypes.DELETED_EVENT, ""),
    WIKI_MOVED("WIKI_MOVED", TapEventTypes.UPDATE_EVENT, ""),
    ARTIFACTS_VERSION_DOWNLOAD_ALLOWED("ARTIFACTS_VERSION_DOWNLOAD_ALLOWED", TapEventTypes.UPDATE_EVENT, ""),
    ARTIFACTS_VERSION_UPDATED("ARTIFACTS_VERSION_UPDATED", TapEventTypes.UPDATE_EVENT, ""),
    FILE_UPDATED("FILE_UPDATED", TapEventTypes.UPDATE_EVENT, ""),
    FILE_COPIED("FILE_COPIED", TapEventTypes.UPDATE_EVENT, ""),
    ITERATION_CREATED("ITERATION_CREATED", TapEventTypes.CREATED_EVENT, "Iterations"),
    WIKI_DELETED("WIKI_DELETED", TapEventTypes.DELETED_EVENT, ""),
    CI_JOB_UPDATED("CI_JOB_UPDATED", TapEventTypes.UPDATE_EVENT, ""),
    CI_JOB_STARTED("CI_JOB_STARTED", TapEventTypes.CREATED_EVENT, ""),
    WIKI_ACCESS_UPDATED("WIKI_ACCESS_UPDATED", TapEventTypes.UPDATE_EVENT, ""),
    MEMBER_ROLE_UPDATED("MEMBER_ROLE_UPDATED", TapEventTypes.UPDATE_EVENT, "ProjectMembers"),
    FILE_RESTORED_FROM_RECYCLE_BIN("FILE_RESTORED_FROM_RECYCLE_BIN", TapEventTypes.UPDATE_EVENT, ""),
    ISSUE_DELETED("ISSUE_DELETED", TapEventTypes.DELETED_EVENT, "Issues"),
    FILE_RENAMED("FILE_RENAMED", TapEventTypes.UPDATE_EVENT, ""),
    ITERATION_PLANNED("ITERATION_PLANNED", TapEventTypes.UPDATE_EVENT, "Issues"),
    ISSUE_ITERATION_CHANGED("ISSUE_ITERATION_CHANGED", TapEventTypes.UPDATE_EVENT, "Issues"),
    WIKI_UPDATED("WIKI_UPDATED", TapEventTypes.UPDATE_EVENT, ""),
    ARTIFACTS_VERSION_DELETED("ARTIFACTS_VERSION_DELETED", TapEventTypes.DELETED_EVENT, ""),
    MEMBER_DELETED("MEMBER_DELETED", TapEventTypes.DELETED_EVENT, "ProjectMembers"),
    ISSUE_STATUS_UPDATED("ISSUE_STATUS_UPDATED", TapEventTypes.UPDATE_EVENT, "Issues"),
    ISSUE_CREATED("ISSUE_CREATED", TapEventTypes.CREATED_EVENT, "Issues"),
    GIT_MR_MERGED("GIT_MR_MERGED", TapEventTypes.UPDATE_EVENT, ""),
    ISSUE_COMMENT_CREATED("ISSUE_COMMENT_CREATED", TapEventTypes.CREATED_EVENT, "Issues"),
    GIT_PUSHED("GIT_PUSHED", TapEventTypes.CREATED_EVENT, ""),
    FILE_DELETED("FILE_DELETED", TapEventTypes.DELETED_EVENT, ""),
    ARTIFACTS_REPO_CREATED("ARTIFACTS_REPO_CREATED", TapEventTypes.CREATED_EVENT, ""),
    ;
    String eventName;//webHook事件类型
    String eventType;//tapEvent事件类型
    String eventGroup;//属于那个表的事件变化

    CodingEvent(String name, String type, String eventGroup) {
        this.eventName = name;
        this.eventType = type;
        this.eventGroup = eventGroup;
    }

    public String getEventGroup() {
        return eventGroup;
    }

    public void setEventGroup(String eventGroup) {
        this.eventGroup = eventGroup;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public static CodingEvent event(String eventName) {
        if (Checker.isEmpty(eventName)) return null;
        CodingEvent[] values = values();
        for (CodingEvent value : values) {
            if (value.eventName.equals(eventName)) {
                return value;
            }
        }
        return null;
    }

    public static String eventType(String eventName) {
        if (Checker.isEmpty(eventName)) return "";
        CodingEvent[] values = values();
        for (CodingEvent value : values) {
            if (value.eventName.equals(eventName)) {
                return value.getEventType();
            }
        }
        return "";
    }
}

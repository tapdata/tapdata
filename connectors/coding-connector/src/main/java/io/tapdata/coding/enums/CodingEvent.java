package io.tapdata.coding.enums;

import io.tapdata.coding.utils.tool.Checker;

public enum  CodingEvent {
    ITERATION_DELETED("ITERATION_DELETED",IssueEventTypes.DELETED_EVENT),
    ARTIFACTS_VERSION_CREATED("ARTIFACTS_VERSION_CREATED",IssueEventTypes.CREATED_EVENT),
    FILE_MOVED("FILE_MOVED",IssueEventTypes.UPDATE_EVENT),
    CI_JOB_CREATED("CI_JOB_CREATED",IssueEventTypes.CREATED_EVENT),
    ARTIFACTS_REPO_DELETED("ARTIFACTS_REPO_DELETED",IssueEventTypes.DELETED_EVENT),
    WIKI_CREATED ("WIKI_CREATED",IssueEventTypes.CREATED_EVENT),
    GIT_MR_CREATED ("GIT_MR_CREATED",IssueEventTypes.CREATED_EVENT),
    GIT_MR_CLOSED("GIT_MR_CLOSED",IssueEventTypes.DELETED_EVENT),
    MEMBER_CREATED("MEMBER_CREATED",IssueEventTypes.CREATED_EVENT),
    GIT_MR_UPDATED("GIT_MR_UPDATED",IssueEventTypes.UPDATE_EVENT),
    CI_JOB_FINISHED("CI_JOB_FINISHED",IssueEventTypes.UPDATE_EVENT),
    WIKI_RESTORED_FROM_RECYCLE_BIN("WIKI_RESTORED_FROM_RECYCLE_BIN",IssueEventTypes.UPDATE_EVENT),
    ISSUE_UPDATED("ISSUE_UPDATED",IssueEventTypes.UPDATE_EVENT),
    FILE_SHARE_UPDATED("FILE_SHARE_UPDATED",IssueEventTypes.UPDATE_EVENT),
    WIKI_MOVED_TO_RECYCLE_BIN("WIKI_MOVED_TO_RECYCLE_BIN",IssueEventTypes.UPDATE_EVENT),
    ISSUE_RELATIONSHIP_CHANGED("ISSUE_RELATIONSHIP_CHANGED",IssueEventTypes.UPDATE_EVENT),
    ARTIFACTS_VERSION_DOWNLOAD_BLOCKED("ARTIFACTS_VERSION_DOWNLOAD_BLOCKED",IssueEventTypes.UPDATE_EVENT),
    WIKI_COPIED("WIKI_COPIED",IssueEventTypes.UPDATE_EVENT),
    ARTIFACTS_VERSION_DOWNLOADED("ARTIFACTS_VERSION_DOWNLOADED",IssueEventTypes.UPDATE_EVENT),
    ISSUE_ASSIGNEE_CHANGED("ISSUE_ASSIGNEE_CHANGED",IssueEventTypes.UPDATE_EVENT),
    ARTIFACTS_VERSION_RELEASED("ARTIFACTS_VERSION_RELEASED",IssueEventTypes.UPDATE_EVENT),
    FILE_MOVED_TO_RECYCLE_BIN("FILE_MOVED_TO_RECYCLE_BIN",IssueEventTypes.UPDATE_EVENT),
    WIKI_SHARE_UPDATED("WIKI_SHARE_UPDATED",IssueEventTypes.UPDATE_EVENT),
    ARTIFACTS_VERSION_DOWNLOAD_FORBIDDEN("ARTIFACTS_VERSION_DOWNLOAD_FORBIDDEN",IssueEventTypes.UPDATE_EVENT),
    ITERATION_UPDATED("ITERATION_UPDATED",IssueEventTypes.UPDATE_EVENT),
    GIT_MR_NOTE("GIT_MR_NOTE",IssueEventTypes.UPDATE_EVENT),
    ISSUE_HOUR_RECORD_UPDATED("ISSUE_HOUR_RECORD_UPDATED",IssueEventTypes.UPDATE_EVENT),
    FILE_CREATED("FILE_CREATED",IssueEventTypes.CREATED_EVENT),
    ARTIFACTS_REPO_UPDATED("ARTIFACTS_REPO_UPDATED",IssueEventTypes.UPDATE_EVENT),
    CI_JOB_DELETED("CI_JOB_DELETED",IssueEventTypes.DELETED_EVENT),
    WIKI_MOVED("WIKI_MOVED",IssueEventTypes.UPDATE_EVENT),
    ARTIFACTS_VERSION_DOWNLOAD_ALLOWED("ARTIFACTS_VERSION_DOWNLOAD_ALLOWED",IssueEventTypes.UPDATE_EVENT),
    ARTIFACTS_VERSION_UPDATED("ARTIFACTS_VERSION_UPDATED",IssueEventTypes.UPDATE_EVENT),
    FILE_UPDATED("FILE_UPDATED",IssueEventTypes.UPDATE_EVENT),
    FILE_COPIED("FILE_COPIED",IssueEventTypes.UPDATE_EVENT),
    ITERATION_CREATED ("ITERATION_CREATED",IssueEventTypes.CREATED_EVENT),
    WIKI_DELETED("WIKI_DELETED",IssueEventTypes.DELETED_EVENT),
    CI_JOB_UPDATED ("CI_JOB_UPDATED",IssueEventTypes.UPDATE_EVENT),
    CI_JOB_STARTED("CI_JOB_STARTED",IssueEventTypes.CREATED_EVENT),
    WIKI_ACCESS_UPDATED ("WIKI_ACCESS_UPDATED",IssueEventTypes.UPDATE_EVENT),
    MEMBER_ROLE_UPDATED("MEMBER_ROLE_UPDATED",IssueEventTypes.UPDATE_EVENT),
    FILE_RESTORED_FROM_RECYCLE_BIN("FILE_RESTORED_FROM_RECYCLE_BIN",IssueEventTypes.UPDATE_EVENT),
    ISSUE_DELETED("ISSUE_DELETED",IssueEventTypes.DELETED_EVENT),
    FILE_RENAMED("FILE_RENAMED",IssueEventTypes.UPDATE_EVENT),
    ITERATION_PLANNED("ITERATION_PLANNED",IssueEventTypes.UPDATE_EVENT),
    ISSUE_ITERATION_CHANGED("ISSUE_ITERATION_CHANGED",IssueEventTypes.UPDATE_EVENT),
    WIKI_UPDATED("WIKI_UPDATED",IssueEventTypes.UPDATE_EVENT),
    ARTIFACTS_VERSION_DELETED("ARTIFACTS_VERSION_DELETED",IssueEventTypes.DELETED_EVENT),
    MEMBER_DELETED("MEMBER_DELETED",IssueEventTypes.DELETED_EVENT),
    ISSUE_STATUS_UPDATED("ISSUE_STATUS_UPDATED",IssueEventTypes.UPDATE_EVENT),
    ISSUE_CREATED("ISSUE_CREATED",IssueEventTypes.UPDATE_EVENT),
    GIT_MR_MERGED("GIT_MR_MERGED",IssueEventTypes.UPDATE_EVENT),
    ISSUE_COMMENT_CREATED("ISSUE_COMMENT_CREATED",IssueEventTypes.UPDATE_EVENT),
    GIT_PUSHED("GIT_PUSHED",IssueEventTypes.CREATED_EVENT),
    FILE_DELETED("FILE_DELETED",IssueEventTypes.DELETED_EVENT),
    ARTIFACTS_REPO_CREATED("ARTIFACTS_REPO_CREATED",IssueEventTypes.CREATED_EVENT),
    ;
    String eventName;
    String eventType;
    CodingEvent(String name,String type){
        this.eventName = name;
        this.eventType = type;
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

    public static CodingEvent event(String eventName){
        if (Checker.isEmpty(eventName)) return null;
        CodingEvent[] values = values();
        for (CodingEvent value : values) {
            if (value.eventName.equals(eventName)){
                return value;
            }
        }
        return null;
    }

    public static String eventType(String eventName){
        if (Checker.isEmpty(eventName)) return "";
        CodingEvent[] values = values();
        for (CodingEvent value : values) {
            if (value.eventName.equals(eventName)){
                return value.getEventType();
            }
        }
        return "";
    }
}

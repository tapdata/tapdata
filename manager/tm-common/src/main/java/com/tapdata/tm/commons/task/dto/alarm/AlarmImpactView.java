package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class AlarmImpactView {
    private String id;
    private String name;
    private String username;
    private String email;
    private String gid;
    private int memberCount;
    private int totalMemberCount;
    private int descendantGroupCount;
    private int directTaskCount;
    private int affectedTasks;
    private int highRiskTasks;
    private boolean cascade;
    private List<GroupRef> groups = new ArrayList<>();

    @Data
    public static class GroupRef {
        private String id;
        private String name;
        private String path;
    }
}

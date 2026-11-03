package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class AlarmReceiverCandidates {
    private List<CandidateUser> users = new ArrayList<>();
    private List<CandidateGroup> groups = new ArrayList<>();

    @Data
    public static class CandidateUser {
        private String id;
        private String username;
        private String email;
    }

    @Data
    public static class CandidateGroup {
        private String id;
        private String name;
        private String gid;
        private String parentId;
        private int validEmailCount;
    }
}

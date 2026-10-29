package com.tapdata.tm.ds.dto;

import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class ConnectionTaskAgentResult {
    private List<String> updatedTaskIds = new ArrayList<>();
    private List<Failure> failures = new ArrayList<>();
    @Data
    public static class Failure {
        private String id;
        private String code;
    }
}

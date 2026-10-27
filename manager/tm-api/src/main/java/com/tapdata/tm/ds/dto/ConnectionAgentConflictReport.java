package com.tapdata.tm.ds.dto;

import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class ConnectionAgentConflictReport {
    private long total;
    private long hiddenCount;
    private boolean hasConflicts;
    private List<Item> items = new ArrayList<>();

    @Data
    public static class Item {
        private String id;
        private String name;
        private String syncType;
        private String status;
        private String code;
        private String reason;
        private List<Connection> connections;
    }

    @Data
    public static class Connection {
        private String id;
        private String name;
    }
}

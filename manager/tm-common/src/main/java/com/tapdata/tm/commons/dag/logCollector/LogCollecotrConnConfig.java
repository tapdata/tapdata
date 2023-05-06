package com.tapdata.tm.commons.dag.logCollector;

import lombok.Data;

import java.util.List;

@Data
public class LogCollecotrConnConfig {
    private String connectionId;
    private List<String> tableNames;
}
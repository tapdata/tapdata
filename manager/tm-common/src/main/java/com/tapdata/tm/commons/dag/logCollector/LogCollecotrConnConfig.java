package com.tapdata.tm.commons.dag.logCollector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogCollecotrConnConfig {
    private String connectionId;
    private List<String> tableNames;
}
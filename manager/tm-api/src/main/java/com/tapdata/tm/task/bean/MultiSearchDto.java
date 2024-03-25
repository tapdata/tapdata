package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.List;

@Data
public class MultiSearchDto {
    private String connectionId;
    private List<String> tableNames;
}

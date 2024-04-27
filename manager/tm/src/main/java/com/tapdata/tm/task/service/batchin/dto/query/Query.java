package com.tapdata.tm.task.service.batchin.dto.query;

import lombok.Data;

import java.util.Map;

@Data
public class Query {
    protected String id;
    protected String projectId;
    protected Map<String, Object> input;
    protected Map<String, Object> output;
}

package com.tapdata.tm.task.service.batchin.dto.scheam;

import lombok.Data;

import java.util.Map;

@Data
public class Schema {
    protected String id;
    protected Map<String, Object> full;
    protected Map<String, Object> imported;
}

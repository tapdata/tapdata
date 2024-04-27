package com.tapdata.tm.task.service.batchin.dto;

import com.tapdata.tm.task.service.batchin.dto.project.Project;
import com.tapdata.tm.task.service.batchin.dto.query.Query;
import com.tapdata.tm.task.service.batchin.dto.scheam.Schema;
import lombok.Data;

import java.util.List;

@Data
public class RelMigBaseDto {
    protected String version;
    protected Project project;
    protected Schema schema;
    protected List<Query> queries;
}

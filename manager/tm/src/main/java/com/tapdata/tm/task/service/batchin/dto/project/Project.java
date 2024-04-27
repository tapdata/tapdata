package com.tapdata.tm.task.service.batchin.dto.project;

import com.tapdata.tm.task.service.batchin.dto.project.content.Content;
import lombok.Data;

import java.util.Map;

@Data
public class Project {
    protected String name;
    protected String type;
    protected String lastModified;
    protected String schemasId;
    protected Content content;
    protected Map<String, Object> connectionDetails;
    protected String id;
}

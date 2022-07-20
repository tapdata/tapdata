package com.tapdata.tm.task.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.MetadataTransformerItemDto;
import com.tapdata.tm.config.security.UserDetail;

public interface TaskNodeService {
    Page<MetadataTransformerItemDto> getNodeTableInfo(String taskId, String nodeId, String searchTableName,
                                                      Integer page, Integer pageSize, UserDetail userDetail);
}

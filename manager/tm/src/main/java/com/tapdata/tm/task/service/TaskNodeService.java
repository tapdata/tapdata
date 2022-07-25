package com.tapdata.tm.task.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.MetadataTransformerItemDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.vo.JsResultDto;
import com.tapdata.tm.task.vo.JsResultVo;

public interface TaskNodeService {
    Page<MetadataTransformerItemDto> getNodeTableInfo(String taskId, String nodeId, String searchTableName,
                                                      Integer page, Integer pageSize, UserDetail userDetail);

    void testRunJsNode(String taskId, String jsNodeId, String tableName, Integer rows, UserDetail userDetail);

    void saveResult(JsResultDto jsResultDto);

    JsResultVo getRun( String taskId, String jsNodeId);
}

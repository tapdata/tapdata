package com.tapdata.tm.task.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.dag.vo.TestRunDto;
import com.tapdata.tm.commons.schema.MetadataTransformerItemDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.vo.JsResultDto;
import com.tapdata.tm.task.vo.JsResultVo;

public interface TaskNodeService {
    Page<MetadataTransformerItemDto> getNodeTableInfo(String taskId, String nodeId, String searchTableName,
                                                      Integer page, Integer pageSize, UserDetail userDetail);

    void testRunJsNode(TestRunDto dto, UserDetail userDetail);

    void saveResult(JsResultDto jsResultDto);

    ResponseMessage<JsResultVo> getRun(String taskId, String jsNodeId, Long version);
}

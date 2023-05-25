package com.tapdata.tm.task.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.dag.vo.TestRunDto;
import com.tapdata.tm.commons.schema.MetadataTransformerItemDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.vo.JsResultDto;
import com.tapdata.tm.task.vo.JsResultVo;

import java.util.Map;

public interface TaskNodeService {
    Page<MetadataTransformerItemDto> getNodeTableInfo(String taskId, String taskRecordId, String nodeId, String searchTableName,
                                                      Integer page, Integer pageSize, UserDetail userDetail);

    void testRunJsNode(TestRunDto dto, UserDetail userDetail, String accessToken);

    Map<String, Object> testRunJsNodeRPC(TestRunDto dto, UserDetail userDetail, String accessToken);

    void saveResult(JsResultDto jsResultDto);

    /**
     * @deprecated 执行试运行后即可获取到试运行结果和试运行日志，无需使用此获取结果，不久的将来会移除这个function
     * */
    ResponseMessage<JsResultVo> getRun(String taskId, String jsNodeId, Long version);

    void checkFieldNode(TaskDto taskDto, UserDetail userDetail);
}

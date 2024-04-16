package com.tapdata.tm.task.dto;

import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import lombok.AllArgsConstructor;
import lombok.Data;

import com.tapdata.tm.commons.task.dto.Message;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Data
@AllArgsConstructor
public class CheckEchoOneNodeParam {
    DataSourceConnectionDto connectionDto;
    DataParentNode<?> dataParentNode;
    List<String> taskProcessIdList;
    Map<String, List<Message>> validateMessage;
    Message message;
    AtomicReference<String> nodeType;
    AtomicReference<String> nodeId;
}
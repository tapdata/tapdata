package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.schema.TapTable;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("custom_processor")
@Getter
@Setter
public class CustomProcessorNode extends JsProcessorNode {
    private String customNodeId;

    private Map<String, Object> form;



    @Override
    protected TapTable getTapTable(Node target, TaskDto taskDtoCopy) {
        return service.loadTapTable(getId(), target.getId(), taskDtoCopy);
    }

    public CustomProcessorNode() {
        super("custom_processor");
    }
}

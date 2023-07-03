package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.process.script.ScriptProcessNode;
import lombok.Getter;
import lombok.Setter;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("js_processor")
@Getter
@Setter
public class JsProcessorNode extends ScriptProcessNode {
    public JsProcessorNode() {
        super("js_processor");
    }

    public JsProcessorNode(String type) {
        super(type);
    }
}

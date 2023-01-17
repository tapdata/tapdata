package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.NodeType;
import lombok.Getter;
import lombok.Setter;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("standard_js_processor")
@Getter
@Setter
public class StandardJsProcessorNode extends JsProcessorNode {

    public StandardJsProcessorNode() {
        super("standard_js_processor");
    }
}

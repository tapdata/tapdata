package com.tapdata.tm.commons.dag.process.script.py;

import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.process.script.ScriptProcessNode;
import lombok.Getter;
import lombok.Setter;

/**
 * @author GavinXiao
 * @description PyProcessNode create by Gavin
 * @create 2023/6/13 19:28
 **/
@NodeType("py_processor")
@Getter
@Setter
public class PyProcessNode extends ScriptProcessNode {
    public PyProcessNode() {
        super(NodeEnum.py_processor.getNodeName());
    }

    public PyProcessNode(String type) {
        super(type);
    }

}

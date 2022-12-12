package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import lombok.Getter;
import lombok.Setter;

@NodeType("union_processor")
@Getter
@Setter
public class UnionProcessorNode extends ProcessorNode{

    private String mergedTableName;

    public UnionProcessorNode() {
        super(NodeEnum.union_processor.name());
    }
}

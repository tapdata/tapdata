package com.tapdata.tm.commons.dag.process;

import cn.hutool.core.lang.Assert;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@NodeType("union_processor")
@Getter
@Setter
public class UnionProcessorNode extends ProcessorNode{

    private String mergedTableName;

    public UnionProcessorNode() {
        super(NodeEnum.union_processor.name());
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema) {
        if (schema != null) {
            return schema;
        }

        Assert.notNull(mergedTableName);
        Assert.notEmpty(inputSchemas);

        return inputSchemas.stream().filter(t -> t.getName().equals(mergedTableName))
                .findFirst().orElseThrow(() -> new RuntimeException("mergedTableName is not find schema"));
    }
}

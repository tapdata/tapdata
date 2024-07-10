package com.tapdata.tm.commons.dag.process;

import com.google.common.collect.Lists;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
@NodeType("migrate_union_processor")
@Getter
@Setter
public class MigrateUnionProcessorNode extends MigrateProcessorNode{
    private String tableName;

    public MigrateUnionProcessorNode() {
        super(NodeEnum.migrate_union_processor.name(), NodeCatalog.processor);
    }

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {
        if (CollectionUtils.isEmpty(inputSchemas)) {
            return Lists.newArrayList();
        }
        List<Schema> outputSchemas = inputSchemas.get(0);
        Schema schema = SchemaUtils.mergeSchema(outputSchemas, null);
        schema.setName(tableName);
        schema.setOriginalName(tableName);
        schema.setAncestorsName(tableName);
        outputSchemas.clear();
        outputSchemas.add(schema);
        return outputSchemas;
    }
}

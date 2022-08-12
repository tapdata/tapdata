package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import lombok.Data;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/9 上午11:50
 */
public abstract class ProcessorNode extends Node<Schema> {

    @EqField
    protected Integer processorThreadNum = 1;


    /**
     * 创建处理器节点
     * @param type 节点类型
     **/
    public ProcessorNode(String type) {
        super(type, NodeCatalog.processor);
    }

    @Override
    protected Schema loadSchema(List<String> includes) {
        return null;
    }

    @Override
    protected Schema saveSchema(Collection<String> predecessors, String nodeId, Schema schema, DAG.Options options) {
        //schema.setTaskId(taskId());
        schema.setNodeId(nodeId);
        List<Schema> result = service.createOrUpdateSchema(ownerId(), null, Collections.singletonList(schema), options, this);

        //service.upsertTransformTemp(this.listener.getSchemaTransformResult(nodeId), this.getDag().getTaskId().toHexString(), nodeId, 1, result, options.getUuid());
        if (result != null && result.size() > 0)
            return result.get(0);
        return schema;
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema) {
        return SchemaUtils.mergeSchema(inputSchemas, schema);
    }

    @Override
    protected Schema cloneSchema(Schema schema) {
        return SchemaUtils.cloneSchema(schema);
    }


    public Integer getProcessorThreadNum() {
        return processorThreadNum;
    }

    public void setProcessorThreadNum(Integer processorThreadNum) {
        this.processorThreadNum = processorThreadNum;
    }
}

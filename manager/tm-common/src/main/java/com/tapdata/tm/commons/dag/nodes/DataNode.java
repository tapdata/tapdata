package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;

import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/9 上午11:55
 */
public abstract class DataNode extends DataParentNode<Schema> {
    public DataNode(String type) {
        super(type);
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        return SchemaUtils.mergeSchema(inputSchemas, schema);
    }

    @Override
    protected Schema cloneSchema(Schema schema) {
        return SchemaUtils.cloneSchema(schema);
    }
}

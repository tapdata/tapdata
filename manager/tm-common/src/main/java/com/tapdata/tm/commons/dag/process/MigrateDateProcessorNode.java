package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;

@NodeType("migrate_date_processor")
@Getter
@Setter
@Slf4j
public class MigrateDateProcessorNode extends MigrateProcessorNode {
    /**
     * 创建处理器节点
     *
     **/
    public MigrateDateProcessorNode() {
        super(NodeEnum.migrate_date_processor.name(), NodeCatalog.processor);
    }

    /** 需要修改时间的类型 */
    private List<String> dataTypes;
    /** 增加或者减少 */
    private boolean add;
    /** 增加或者减少的小时数 */
    private int hours;

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {
        //js节点的模型可以是直接虚拟跑出来的。 跑出来就是正确的模型，由引擎负责传值给tm
        if (CollectionUtils.isNotEmpty(schemas)) {
            return schemas;
        }

        List<Schema> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(inputSchemas)) {
            for (List<Schema> inputSchema : inputSchemas) {
                result.addAll(inputSchema);
            }
        }
        return result;
    }

    @Override
    protected List<Schema> loadSchema(List<String> includes) {
        return null;
    }

    @Override
    protected List<Schema> saveSchema(Collection<String> predecessors, String nodeId, List<Schema> schema, DAG.Options options) {

        schema.forEach(s -> {
            //s.setTaskId(taskId);
            s.setNodeId(nodeId);
        });

        return service.createOrUpdateSchema(ownerId(), toObjectId(getConnectId()), schema, options, this);
    }

    @Override
    protected List<Schema> cloneSchema(List<Schema> schemas) {
        if (schemas == null) {
            return Collections.emptyList();
        }
        return SchemaUtils.cloneSchema(schemas);
    }

    private String getConnectId() {
        AtomicReference<String> connectionId = new AtomicReference<>("");

        getSourceNode().stream().findFirst().ifPresent(node -> connectionId.set(node.getConnectionId()));
        return connectionId.get();
    }
}

package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.schema.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collection;
import java.util.List;



/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/17 21:00 Create
 */
@NodeType("auto_inspect")
@Getter
@Setter
@ToString
public class AutoInspectNode extends DataParentNode<List<Schema>> {
    private String targetNodeId;
    private DatabaseNode fromNode;
    private DatabaseNode toNode;
    private List<SyncObjects> syncObjects;

    public AutoInspectNode() {
        super(AutoInspectConstants.NODE_TYPE);
    }

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas) {
        return null;
    }

    @Override
    protected List<Schema> loadSchema(List<String> includes) {
        return null;
    }

    @Override
    protected List<Schema> saveSchema(Collection<String> predecessors, String nodeId, List<Schema> schema, DAG.Options options) {
        return null;
    }

    @Override
    protected List<Schema> cloneSchema(List<Schema> schemas) {
        return null;
    }
}

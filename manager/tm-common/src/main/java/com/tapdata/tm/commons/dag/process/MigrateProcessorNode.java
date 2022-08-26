package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.schema.Schema;

import java.util.*;

public abstract class MigrateProcessorNode extends Node<List<Schema>> {
    /**
     * 创建处理器节点
     *
     **/
    public MigrateProcessorNode(String type, NodeCatalog catalog) {
        super(type, catalog);
    }
}

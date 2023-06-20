package com.tapdata.tm.commons.dag.process.script.py;

import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.process.script.MigrateScriptProcessNode;

/**
 * @author GavinXiao
 * @description MigratePyProcessNode create by Gavin
 * @create 2023/6/19 18:32
 **/
public class MigratePyProcessNode extends MigrateScriptProcessNode {

    /**
     * 创建python处理器节点
     *
     * @param type
     * @param catalog
     */
    public MigratePyProcessNode(String type, NodeCatalog catalog) {
        super(type, catalog);
    }

    public MigratePyProcessNode() {
        super(NodeEnum.migrate_py_processor.name(), NodeCatalog.processor);
    }
}

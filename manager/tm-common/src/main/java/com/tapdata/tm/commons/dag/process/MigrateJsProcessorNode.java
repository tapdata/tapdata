package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.process.script.MigrateScriptProcessNode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("migrate_js_processor")
@Getter
@Setter
@Slf4j
public class MigrateJsProcessorNode extends MigrateScriptProcessNode {
    public MigrateJsProcessorNode(String type, NodeCatalog catalog) {
        super(type, catalog);
    }

    public MigrateJsProcessorNode() {
        super(NodeEnum.migrate_js_processor.name(), NodeCatalog.processor);
    }
}

package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("standard_migrate_js_processor")
@Getter
@Setter
@Slf4j
public class StandardMigrateJsProcessorNode extends MigrateJsProcessorNode {

    public StandardMigrateJsProcessorNode() {
        super(NodeEnum.standard_migrate_js_processor.name(), NodeCatalog.processor);
    }

}

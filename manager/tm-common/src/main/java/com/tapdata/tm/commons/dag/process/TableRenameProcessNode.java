package com.tapdata.tm.commons.dag.process;


import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;

@NodeType("table_rename_processor")
@Getter
@Setter
@Slf4j
public class TableRenameProcessNode extends ProcessorNode {
    /**
     * 创建处理器节点
     *
     **/
    public TableRenameProcessNode() {
        super(NodeEnum.table_rename_processor.name());
    }

    /**
     * 源表名-新表名
     */
    private LinkedHashMap<String, String> tableNames;

    /**
     * 替换关键字
     */
    private String searchString;
    /**
     * 替换文本
     */
    private String replacement;

    /**
     * 前缀
     */
    private String prefix;
    /**
     * 后缀
     */
    private String suffix;

    /**
     * Capitalized toUpperCase 转大写 toLowerCase 转小写 ""不变（默认）
     */
    private String capitalized;

}

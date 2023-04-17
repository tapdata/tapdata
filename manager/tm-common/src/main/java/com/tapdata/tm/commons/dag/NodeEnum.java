package com.tapdata.tm.commons.dag;

import lombok.Getter;

/**
 * @Author: Zed
 * @Date: 2022/3/17
 * @Description:
 */
public enum NodeEnum {
    mem_cache("共享缓存节点"),
    database("数据源节点"),
    table("表节点"),
    hazelcastIMDG("共享挖掘赋值节点"),
    logCollector("共享挖掘节点"),
    aggregation_processor("聚合节点"),
    cache_lookup_processor(""),
    custom_processor("自定义节点"),
    field_add_del_processor("字段增删节点"),
    field_calc_processor("字段计算节点"),
    field_mod_type_processor("字段类型修改节点"),
    field_processor("字段处理器节点"),
    field_rename_processor("字段改名节点"),
    date_processor("开发日期编辑节点"),
    join_processor("Join节点"),
    js_processor("增强JS节点"),
    standard_js_processor("标准JS节点"),
    merge_table_processor("多表合并节点"),
    row_filter_processor("Row Filter节点"),
    table_rename_processor("表编辑节点"),
    migrate_field_rename_processor("字段编辑节点"),
    migrate_date_processor("复制日期编辑节点"),
    migrate_js_processor("增强JS节点"),
    standard_migrate_js_processor("标准JS节点"),
    union_processor("Union节点");
    
    @Getter
    private String nodeName;

    NodeEnum(String nodeName) {
        this.nodeName = nodeName;
    }
}

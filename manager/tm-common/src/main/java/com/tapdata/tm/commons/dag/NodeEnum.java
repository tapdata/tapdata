package com.tapdata.tm.commons.dag;

/**
 * @Author: Zed
 * @Date: 2022/3/17
 * @Description:
 */
public enum NodeEnum {
    /** 共享缓存节点 */
    mem_cache,
    /** 数据源节点 */
    database,
    /** 表节点 */
    table,
    /** 共享挖掘赋值节点 */
    hazelcastIMDG,
    /** 共享挖掘节点*/
    logCollector,
    /** */
    aggregation_processor,
    /** */
    cache_lookup_processor,
    /** 自定义节点*/
    custom_processor,
    /** 字段新增山粗节点*/
    field_add_del_processor,
    /** 字段计算节点*/
    field_calc_processor,
    /** 字段类型修改节点*/
    field_mod_type_processor,
    /** 字段处理器节点*/
    field_processor,
    /** 字段改名节点*/
    field_rename_processor,
    /** join节点*/
    join_processor,
    /** js节点*/
    js_processor,

    standard_js_processor,
    /** 多表合并节点*/
    merge_table_processor,
    /** */
    row_filter_processor,

    /**
     * 表编辑节点
     */
    table_rename_processor,

    /**
     * 数据复制-字段编辑节点
     */
    migrate_field_rename_processor,
    migrate_js_processor,
    standard_migrate_js_processor,
    union_processor
}

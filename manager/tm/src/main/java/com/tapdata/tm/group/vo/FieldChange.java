package com.tapdata.tm.group.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 单个字段的变更信息
 * <ul>
 *   <li>{@code field}：字段路径，简单字段如 {@code "readBatchSize"}，
 *       DAG 节点内字段如 {@code "dag.nodes.PGshare.tableNames"}</li>
 *   <li>{@code from}：导入前 DB 中的值</li>
 *   <li>{@code to}：导入文件中的值</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldChange {
    private String field;
    private Object from;
    private Object to;
}
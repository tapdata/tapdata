package com.tapdata.tm.commons.dag.vo;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * 目标节点字段顺序配置，按表分组。
 * 表名、字段名均兼容改名前后（参见 DataParentNode#sortFieldsByPostion）。
 */
@Getter
@Setter
@ToString
public class FieldPostion implements Serializable {
    /** 目标表名（兼容源表名/原始表名） */
    private String tableName;
    /** 该表的字段顺序配置 */
    private List<Postion> fields;

    @Getter
    @Setter
    @ToString
    public static class Postion implements Serializable {
        private String fieldName;
        private Integer columnPosition;
    }
}

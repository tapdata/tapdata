package com.tapdata.tm.commons.schema.bean;

import lombok.Data;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/9/18
 * @Description:
 */
@Data
public class ForeignKeyTable {
    private String id;
    private String rel;
    private List<RelationField> fields;
}

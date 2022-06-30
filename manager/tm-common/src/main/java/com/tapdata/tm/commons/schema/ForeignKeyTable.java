package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.schema.bean.RelationField;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/9/18
 * @Description:
 */
@Data
public class ForeignKeyTable implements Serializable {
    private String id;
    private String rel;
    private List<RelationField> fields;
}

package com.tapdata.tm.commons.schema;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/9/29
 * @Description:
 */
@Data
public class TableIndex implements Serializable {
    private String indexName;

    private String indexType;

    private String indexSourceType;

    private boolean unique;

    private List<TableIndexColumn> columns;

    private String dbIndexDescriptionJson;

    private String primaryKey;
    private String clustered;
}

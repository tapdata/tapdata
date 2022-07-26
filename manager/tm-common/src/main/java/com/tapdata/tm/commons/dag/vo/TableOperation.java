package com.tapdata.tm.commons.dag.vo;

import lombok.Data;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/3/4 上午9:41
 */
@Data
public class TableOperation {

    public static final String OPERATION_RENAME = "rename";

    // 操作类型：rename
    private String type;

    /*- rename operation config start ***/
    private String originalTableName;
    private String tableName;
    /*- rename operation config end ***/
}

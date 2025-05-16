package com.tapdata.tm.taskinspect.vo;

import lombok.Data;

/**
 * 上报差异修复结果
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/28 18:54 Create
 */
@Data
public class ResultsRecoverVo {
    private String sourceTable;
    private String rowId;

    public static ResultsRecoverVo of(String table, String rowId) {
        ResultsRecoverVo vo = new ResultsRecoverVo();
        vo.setSourceTable(table);
        vo.setRowId(rowId);
        return vo;
    }
}

package com.tapdata.tm.inspect.vo;

import lombok.Data;

import java.io.Serializable;

/**
 * 错误表和行统计结果
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/12 14:58 Create
 */
@Data
public class FailTableAndRowsVo implements Serializable {
    private Integer tableTotals;
    private Long rowsTotals;
}

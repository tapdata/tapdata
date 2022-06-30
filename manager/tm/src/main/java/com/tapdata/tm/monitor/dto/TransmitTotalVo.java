package com.tapdata.tm.monitor.dto;

import lombok.Data;

import java.util.Date;
import java.util.Map;

/**
 * 要统计的几个数据
 * "inputTotal",
 * "outputTotal",
 * "insertedTotal",
 * "updatedTotal",
 * "deletedTotal"
 *
 * @param userDetail
 * @return
 */
@Data
public class TransmitTotalVo {
    private Integer inputTotal;
    private Integer outputTotal;
    private Integer insertedTotal;
    private Integer updatedTotal;
    private Integer deletedTotal;
}

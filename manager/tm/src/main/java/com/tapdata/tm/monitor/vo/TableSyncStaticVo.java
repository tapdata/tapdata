package com.tapdata.tm.monitor.vo;/**
 * Created by jiuyetx on 2022/8/12 17:43
 */

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author jiuyetx
 * @date 2022/8/12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableSyncStaticVo {
    @Schema(description = "任务记录id")
    private String id;
    @Schema(description = "源表名")
    private String originTable;
    @Schema(description = "目标表名")
    private String targetTable;
    @Schema(description = "表结构同步")
    private String constructSyncSatus;
    @Schema(description = "数据同步")
    private BigDecimal syncRate;
    @Schema(description = "全量同步状态 NOSTART:未同步 PAUSE:已停止 DONE:已完成 ING:同步中")
    private String fullSyncStatus;
}

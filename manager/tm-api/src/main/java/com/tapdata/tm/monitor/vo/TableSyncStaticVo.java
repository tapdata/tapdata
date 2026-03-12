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

    public interface FullSyncStatus {
        String DONE = "DONE"; // 完成
        String NOT_START = "NOT_START"; // 未同步
        String COUNTING = "COUNTING"; // 统计中
        String ING = "ING"; // 同步中
        String ERROR_SKIPPED = "ERROR_SKIPPED"; // 错误跳过
    }

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
    @Schema(description = "全量同步状态 NOT_START:未同步 PAUSE:已停止 DONE:已完成 ING:同步中 COUNTING:统计中 ERROR_SKIPPED:错误表跳过")
    private String fullSyncStatus;
}

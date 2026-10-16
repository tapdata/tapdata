package com.tapdata.tm.notification.record.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReadStatePreviewVo {
    /** 当前筛选下将被修改的记录数 */
    private int count;
    private List<String> ids;
    /** 超过单次上限时为 true，前端需提示分批 */
    private boolean truncated;
}

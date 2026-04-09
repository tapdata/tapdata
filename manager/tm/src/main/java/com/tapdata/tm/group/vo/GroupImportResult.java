package com.tapdata.tm.group.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 导入操作的返回结果，包含记录 ID 和本次导入涉及的变更 diff。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GroupImportResult {
    /** 导入记录 ID，可通过 getGroupImportStatus/{id} 轮询进度 */
    private String recordId;
    /** 本次导入相对于当前系统状态的变更内容 */
    private ResourceDiff diff;
}
package com.tapdata.tm.notification.record.dto;

import lombok.Data;

import java.util.List;

/**
 * 批量已读/未读必须给出明确的记录 ID；“全部”先调用预览接口取得当前筛选下的 ID 与数量。
 */
@Data
public class ReadStateRequest {
    private List<String> ids;
    private Boolean read;
}

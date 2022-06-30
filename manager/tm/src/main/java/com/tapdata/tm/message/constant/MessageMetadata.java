package com.tapdata.tm.message.constant;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 产生消息的元数据信息
 * 如  任务中断，则name 为任务名称，id 为任务id
 *     agent停止  则agent为agent 名称  id 为agent id
 */
@Data
@AllArgsConstructor
public class MessageMetadata {
    private String name;
    private String id;
}

package com.tapdata.tm.group.strategy;

import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.group.dto.GroupInfoRecordDetail.RecordAction;
import com.tapdata.tm.group.dto.ResourceType;

/**
 * 导入策略接口
 * 定义不同导入模式的处理规范
 */
public interface ImportStrategy {

    /**
     * 获取当前策略对应的导入模式
     * 
     * @return 导入模式
     */
    ImportModeEnum getImportMode();

    /**
     * 获取该策略的默认记录动作
     * 
     * @return 默认记录动作
     */
    RecordAction getDefaultAction();

    /**
     * 处理重名资源时的记录动作
     * 
     * @param resourceType 资源类型
     * @return 记录动作
     */
    RecordAction handleDuplicate(ResourceType resourceType);

    /**
     * 获取重复资源的消息描述
     * 
     * @param resourceType 资源类型
     * @return 消息描述
     */
    String getDuplicateMessage(ResourceType resourceType);

    /**
     * 是否需要重命名现有资源
     * 
     * @param resourceType 资源类型
     * @return true 需要重命名现有资源
     */
    boolean shouldRenameExisting(ResourceType resourceType);

    /**
     * 是否跳过重复资源
     * 
     * @param resourceType 资源类型
     * @return true 跳过导入
     */
    boolean shouldSkipDuplicate(ResourceType resourceType);
}

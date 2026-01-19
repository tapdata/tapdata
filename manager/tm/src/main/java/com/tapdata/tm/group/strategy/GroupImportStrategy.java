package com.tapdata.tm.group.strategy;

import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.group.dto.GroupInfoRecordDetail.RecordAction;
import com.tapdata.tm.group.dto.ResourceType;
import org.springframework.stereotype.Component;

/**
 * GROUP_IMPORT 导入策略
 * 任务重名时重命名现有任务后导入新任务
 * 模块重名时跳过导入
 */
@Component
public class GroupImportStrategy implements ImportStrategy {

    @Override
    public ImportModeEnum getImportMode() {
        return ImportModeEnum.GROUP_IMPORT;
    }

    @Override
    public RecordAction getDefaultAction() {
        return RecordAction.IMPORTED;
    }

    @Override
    public RecordAction handleDuplicate(ResourceType resourceType) {
        return RecordAction.REPLACED;
    }

    @Override
    public String getDuplicateMessage(ResourceType resourceType) {
        if (resourceType == ResourceType.MODULE) {
            return "duplicate module, replaced existing";
        } else {
            return "duplicate task, replaced existing";
        }
    }

    @Override
    public boolean shouldRenameExisting(ResourceType resourceType) {
        // 任务类型需要重命名现有资源
        return resourceType == ResourceType.MIGRATE_TASK || resourceType == ResourceType.SYNC_TASK;
    }

    @Override
    public boolean shouldSkipDuplicate(ResourceType resourceType) {
        // 模块类型跳过重复
        return resourceType == ResourceType.MODULE;
    }
}

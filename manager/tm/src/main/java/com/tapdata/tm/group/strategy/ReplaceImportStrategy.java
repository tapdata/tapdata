package com.tapdata.tm.group.strategy;

import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.group.dto.GroupInfoRecordDetail.RecordAction;
import com.tapdata.tm.group.dto.ResourceType;
import org.springframework.stereotype.Component;

/**
 * REPLACE 导入策略
 * 所有资源重名时都替换现有资源
 */
@Component
public class ReplaceImportStrategy implements ImportStrategy {

    @Override
    public ImportModeEnum getImportMode() {
        return ImportModeEnum.REPLACE;
    }

    @Override
    public RecordAction getDefaultAction() {
        return RecordAction.REPLACED;
    }

    @Override
    public RecordAction handleDuplicate(ResourceType resourceType) {
        // 所有资源类型都替换
        return RecordAction.REPLACED;
    }

    @Override
    public String getDuplicateMessage(ResourceType resourceType) {
        return "duplicate, replaced existing";
    }

    @Override
    public boolean shouldRenameExisting(ResourceType resourceType) {
        // 不需要重命名，直接替换
        return false;
    }

    @Override
    public boolean shouldSkipDuplicate(ResourceType resourceType) {
        // 不跳过，直接替换
        return false;
    }
}

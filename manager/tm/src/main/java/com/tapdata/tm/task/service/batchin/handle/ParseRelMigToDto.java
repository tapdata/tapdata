package com.tapdata.tm.task.service.batchin.handle;

import com.tapdata.tm.task.service.batchin.dto.RelMigBaseDto;

public class ParseRelMigToDto {
    public static RelMigBaseDto parseToBaseDto(String relMigJson) {
        return new RelMigBaseDto();
    }
}

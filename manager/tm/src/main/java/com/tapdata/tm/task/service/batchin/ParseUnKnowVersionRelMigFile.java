package com.tapdata.tm.task.service.batchin;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.batchin.constant.KeyWords;
import com.tapdata.tm.task.service.batchin.constant.ParseRelMigFileVersionMapping;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

public class ParseUnKnowVersionRelMigFile implements ParseRelMig<TaskDto> {
    ParseParam param;
    Map<String, Object> relMigInfo;
    public ParseUnKnowVersionRelMigFile(ParseParam param) {
        this.param = param;
        this.relMigInfo = Optional.ofNullable(param.getRelMigInfo()).orElse(new HashMap<>());
    }
    @Override
    public List<TaskDto> parse() {
        String version = String.valueOf(this.relMigInfo.get(KeyWords.VERSION));
        throw new BizException("relMig.parse.unSupport", version, getSupportedVersion());
    }
    private String getSupportedVersion() {
        ParseRelMigFileVersionMapping[] values = ParseRelMigFileVersionMapping.values();
        StringJoiner builder = new StringJoiner(", ");
        for (ParseRelMigFileVersionMapping value : values) {
            if (!ParseRelMigFileVersionMapping.UN_KNOW.equals(value)) {
                builder.add(value.getVersion());
            }
        }
        return builder.toString();
    }
}

package com.tapdata.tm.task.param;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Dexter
 */
@Data
public class LogSettingParam {
    private String level;
    private Long recordCeiling;
    private Long intervalCeiling;
    private String query;

    public String getLevel() {
        if (StringUtils.equalsAnyIgnoreCase(level, "DEBUG", "INFO", "WARN", "ERROR")) {
            return level.toUpperCase();
        }

        throw new RuntimeException("Invalid value for level");
    }
}

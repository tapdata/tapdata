package com.tapdata.tm.commons.task.dto.alarm;

import cn.hutool.core.date.DateUnit;
import com.tapdata.tm.commons.task.constant.AlarmSettingTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Getter
@Setter
@AllArgsConstructor
public class AlarmSettingDto {
    private AlarmSettingTypeEnum type;
    private boolean open;
    private String key;
    private int sort;
    private boolean systemNotify;
    private boolean emailNotify;
    private int interval;
    private DateUnit unit;
}

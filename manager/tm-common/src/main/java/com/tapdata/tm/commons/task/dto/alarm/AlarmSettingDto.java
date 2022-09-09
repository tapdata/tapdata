package com.tapdata.tm.commons.task.dto.alarm;

import cn.hutool.core.date.DateUnit;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.AlarmSettingTypeEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import lombok.*;

import java.util.List;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlarmSettingDto {
    private AlarmSettingTypeEnum type;
    private boolean open;
    private AlarmKeyEnum key;
    private int sort;
    private List<NotifyEnum> notify;
    private int interval;
    private DateUnit unit;
}

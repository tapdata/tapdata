package com.tapdata.tm.commons.task.dto.alarm;

import cn.hutool.core.date.DateUnit;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.AlarmSettingTypeEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.List;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlarmSettingDto extends BaseDto implements Serializable {
    private AlarmSettingTypeEnum type;
    private boolean open;
    private AlarmKeyEnum key;
    private int sort;
    private List<NotifyEnum> notify;
    private int interval;
    private DateUnit unit;
}

package com.tapdata.tm.Settings.entity;

import cn.hutool.core.date.DateUnit;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.AlarmSettingTypeEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@Document("Settings_Alarm")
public class AlarmSetting extends BaseEntity {
    private AlarmSettingTypeEnum type;
    private boolean open;
    private AlarmKeyEnum key;
    private int sort;
    private List<NotifyEnum> notify;
    private int interval;
    private DateUnit unit;
}

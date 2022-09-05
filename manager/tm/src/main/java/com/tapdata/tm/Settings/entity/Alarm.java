package com.tapdata.tm.Settings.entity;

import cn.hutool.core.date.DateUnit;
import com.tapdata.tm.Settings.constant.AlarmTypeEnum;
import com.tapdata.tm.base.entity.BaseEntity;
import lombok.*;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Document("Settings_Alarm")
public class Alarm extends BaseEntity {
    private AlarmTypeEnum type;
    private String key;
    private int sort;
    private boolean systemNotify;
    private boolean emailNotify;
    private int interval;
    private DateUnit unit;
}

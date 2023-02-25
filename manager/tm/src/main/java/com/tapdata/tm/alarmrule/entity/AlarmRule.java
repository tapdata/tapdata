package com.tapdata.tm.alarmrule.entity;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.base.entity.BaseEntity;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@Document("Settings_Alarm_Rule")
public class AlarmRule extends BaseEntity {
    private AlarmKeyEnum key;
    private int point;
    @Schema(description = "-1 小于等于 <= ; 0; 1 大于等于 >= ")
    private int equalsFlag;
    @Schema(description = "毫秒")
    private int ms;
}

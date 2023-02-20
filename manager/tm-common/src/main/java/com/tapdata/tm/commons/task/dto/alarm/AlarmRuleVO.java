package com.tapdata.tm.commons.task.dto.alarm;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AlarmRuleVO implements Serializable {
    private AlarmKeyEnum key;
    private int point;
    //-1 小于等于 <= ; 0; 1 大于等于 >=
    private int equalsFlag;
    //毫秒
    private int ms;
}

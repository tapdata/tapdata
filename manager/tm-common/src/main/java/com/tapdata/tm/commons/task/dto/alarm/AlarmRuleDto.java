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
@EqualsAndHashCode(callSuper = true)
public class AlarmRuleDto extends BaseDto implements Serializable {
    private AlarmKeyEnum key;
    private int point;
    //-1 小于等于 <= ; 0; 1 大于等于 >=
    private int equalsFlag;
    //毫秒
    private int ms;
    //连续次数
    private int times;

    /**
     * 对比值，double/int/long...
     * */
    private Number value;

    /**
     * 对比值单位：ms、%、MB
     */
    private String unit;

    String type;

    public void setKey(AlarmKeyEnum key) {
        this.key = key;
				if (null != key) {
					this.type = key.getType();
				}
    }

    public String toEqualsFlag() {
        if (equalsFlag == 0) {
            return "=";
        }
        if (equalsFlag > 0) {
            return ">=";
        } else {
            return "<=";
        }
    }
}

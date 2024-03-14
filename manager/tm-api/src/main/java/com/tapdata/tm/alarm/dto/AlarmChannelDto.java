package com.tapdata.tm.alarm.dto;

import lombok.Data;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/2/24 上午11:09
 */
@Data
public class AlarmChannelDto {
    private String type;

    public AlarmChannelDto(String type) {
        this.type = type;
    }
}

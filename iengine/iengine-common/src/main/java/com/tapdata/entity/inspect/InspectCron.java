package com.tapdata.entity.inspect;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class InspectCron implements Serializable {
    private Integer scheduleTimes;
    private Integer intervals;  // 间隔时间
    private String intervalsUnit;  // second、minute、hour、day、week、month
}

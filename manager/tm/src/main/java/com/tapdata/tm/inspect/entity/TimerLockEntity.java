package com.tapdata.tm.inspect.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class TimerLockEntity extends BaseEntity {
    private String hour;
    private String tmProcessName;
}

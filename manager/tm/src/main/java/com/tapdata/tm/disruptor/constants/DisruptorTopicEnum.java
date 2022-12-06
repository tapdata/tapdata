package com.tapdata.tm.disruptor.constants;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum DisruptorTopicEnum {
    CREATE_RECORD("createRecordEventHandler"),TASK_STATUS("updateRecordStatusEventHandler");

    private final String beanName;
}

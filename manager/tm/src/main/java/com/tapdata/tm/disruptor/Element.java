package com.tapdata.tm.disruptor;

import com.tapdata.tm.disruptor.constants.DisruptorTopicEnum;
import lombok.Data;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Data
public class Element<T> {
    private DisruptorTopicEnum topic;
    private T data;
}

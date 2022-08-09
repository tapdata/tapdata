package com.tapdata.tm.disruptor;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
public class ObjectEvent<T> {
    private T event;
}

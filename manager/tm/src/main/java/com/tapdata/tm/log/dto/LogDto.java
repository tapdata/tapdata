package com.tapdata.tm.log.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;


/**
 * Logs
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class LogDto extends BaseDto {

    private Object level;

    private Object loggerName;

    private Object message;

    private Object date;

    private Object thrown;

    private Object threadName;

    private Object contextMap;

    private Object contextStack;

    private Object ip;

    private Object uuid;

    private Object hostname;

    private Object accessCode;

    private Object username;

    private Object threadId;

    private Object threadPriority;

    private Object millis;


}

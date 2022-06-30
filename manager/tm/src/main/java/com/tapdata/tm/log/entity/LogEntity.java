package com.tapdata.tm.log.entity;

import java.util.Date;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * Logs
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Logs")
public class LogEntity extends BaseEntity {

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
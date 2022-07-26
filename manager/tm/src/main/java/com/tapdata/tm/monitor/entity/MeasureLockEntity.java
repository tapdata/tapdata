package com.tapdata.tm.monitor.entity;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@Data
@Document("MeasureLock")
public class MeasureLockEntity {
    private Date time;
    private String granularity;
    private String unique;
    private Date createdTime;
}

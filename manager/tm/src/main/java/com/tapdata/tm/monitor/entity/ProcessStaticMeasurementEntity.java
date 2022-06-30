package com.tapdata.tm.monitor.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.Map;

@Document("ProcessStaticMeasurement")
@Data
public class ProcessStaticMeasurementEntity{
    private Map tags;
    private Date createTime;
    private Map values;
}

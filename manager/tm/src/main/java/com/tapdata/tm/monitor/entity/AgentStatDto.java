package com.tapdata.tm.monitor.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * @Author: Zed
 * @Date: 2022/3/15
 * @Description:
 */
@Data
@Document("AgentStatistics")
public class AgentStatDto {
    private ObjectId id;
    private StatTags tags;
    private Statistics statistics;
    private Date date;
}

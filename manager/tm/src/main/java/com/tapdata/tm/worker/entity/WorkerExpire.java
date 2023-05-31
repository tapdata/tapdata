package com.tapdata.tm.worker.entity;


import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("WorkerExpire")
public class WorkerExpire extends BaseEntity {
    @Indexed(unique = true)
    private String userId;
    private String shareUser;
    private Date expireTime;
}

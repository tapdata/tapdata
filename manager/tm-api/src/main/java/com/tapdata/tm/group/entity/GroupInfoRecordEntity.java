package com.tapdata.tm.group.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import com.tapdata.tm.group.dto.GroupInfoRecordDetail;

import java.util.Date;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("GroupInfoRecord")
public class GroupInfoRecordEntity extends BaseEntity {
    private String type;
    private Date operationTime;
    private String operator;
    private String status;
    private String message;
    private String fileName;
    private List<GroupInfoRecordDetail> details;
    private Integer progress;
}

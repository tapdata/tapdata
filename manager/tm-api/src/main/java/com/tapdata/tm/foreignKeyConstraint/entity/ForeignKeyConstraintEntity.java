package com.tapdata.tm.foreignKeyConstraint.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("ForeignKeyConstraint")
public class ForeignKeyConstraintEntity extends BaseEntity {
    private String taskId;
    private List<String> sqlList;
}

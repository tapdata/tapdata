package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.IDataPermissionEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;

/**
 * Project
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Project")
public class ProjectEntity extends BaseEntity implements IDataPermissionEntity {
    // 项目名称
    @Indexed(unique = true)
    private String name;

    // 项目描述
    private String description;

    // 定时启动规则
    private String crontabExpression;

    // 状态
    private String status;

    // 删除标记
    @Indexed
    private boolean isDeleted;

    // 任务的ObjectId列表
    private List<ObjectId> tasks;

    // 任务依赖信息，键为任务的ObjectId，值为依赖信息
    private Map<ObjectId, String> depends;

} 
package com.tapdata.tm.task.service;

import org.bson.types.ObjectId;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.ProjectDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.entity.ProjectEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.ProjectRepository;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.config.security.UserDetail;

import lombok.NonNull;

public abstract class ProjectService extends BaseService<ProjectDto, ProjectEntity, ObjectId, ProjectRepository> {
    public ProjectService(@NonNull ProjectRepository repository) {
        super(repository, ProjectDto.class, ProjectEntity.class);
    }

    public abstract ProjectDto create(ProjectDto projectDto, UserDetail user);

    public abstract ProjectDto update(ProjectDto projectDto, UserDetail user);

    public abstract ProjectDto remove(ObjectId id, UserDetail user);

    public abstract void start(ObjectId id, UserDetail user);

    public abstract void stop(ObjectId id, UserDetail user);

    public abstract void renew(ObjectId id, UserDetail user);

}

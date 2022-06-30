package com.tapdata.tm.jobddlhistories.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.jobddlhistories.dto.JobDDLHistoriesDto;
import com.tapdata.tm.jobddlhistories.entity.JobDDLHistoriesEntity;
import com.tapdata.tm.jobddlhistories.repository.JobDDLHistoriesRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Service
@Slf4j
public class JobDDLHistoriesService extends BaseService<JobDDLHistoriesDto, JobDDLHistoriesEntity, ObjectId, JobDDLHistoriesRepository> {
    public JobDDLHistoriesService(@NonNull JobDDLHistoriesRepository repository) {
        super(repository, JobDDLHistoriesDto.class, JobDDLHistoriesEntity.class);
    }

    protected void beforeSave(JobDDLHistoriesDto jobDDLHistories, UserDetail user) {

    }
}
package com.tapdata.tm.job.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.job.dto.JobDto;
import com.tapdata.tm.job.entity.JobEntity;
import com.tapdata.tm.job.repository.JobRepository;
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
public class JobService extends BaseService<JobDto, JobEntity, ObjectId, JobRepository> {
    public JobService(@NonNull JobRepository repository) {
        super(repository, JobDto.class, JobEntity.class);
    }

    protected void beforeSave(JobDto jobs, UserDetail user) {

    }
}
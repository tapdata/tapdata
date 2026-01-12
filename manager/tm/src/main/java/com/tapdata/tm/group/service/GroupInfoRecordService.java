package com.tapdata.tm.group.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.dto.GroupInfoRecordDto;
import com.tapdata.tm.group.entity.GroupInfoRecordEntity;
import com.tapdata.tm.group.repostitory.GroupInfoRecordRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GroupInfoRecordService extends BaseService<GroupInfoRecordDto, GroupInfoRecordEntity, ObjectId, GroupInfoRecordRepository> {

    public GroupInfoRecordService(@NonNull GroupInfoRecordRepository repository) {
        super(repository, GroupInfoRecordDto.class, GroupInfoRecordEntity.class);
    }

    @Override
    protected void beforeSave(GroupInfoRecordDto dto, UserDetail userDetail) {
    }
}

package com.tapdata.tm.alarmMail.service;

import com.tapdata.tm.alarmMail.dto.AlarmMailDto;
import com.tapdata.tm.alarmMail.entity.AlarmMail;
import com.tapdata.tm.alarmMail.repository.AlarmMailRepository;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

public abstract class AlarmMailService extends BaseService<AlarmMailDto, AlarmMail, ObjectId, AlarmMailRepository> {
    public AlarmMailService(@NonNull AlarmMailRepository repository) {
        super(repository, AlarmMailDto.class, AlarmMail.class);
    }

    @Override
    public abstract void beforeSave(AlarmMailDto dto, UserDetail userDetail);
}

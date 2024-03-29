package com.tapdata.tm.alarmMail.service;

import com.tapdata.tm.alarmMail.dto.AlarmMailDto;
import com.tapdata.tm.alarmMail.repository.AlarmMailRepository;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmMailServiceImpl extends AlarmMailService {
    public AlarmMailServiceImpl(@NonNull AlarmMailRepository repository) {
        super(repository);
    }

    @Override
    public void beforeSave(AlarmMailDto dto, UserDetail userDetail) {
        return;
    }

    public <T extends BaseDto> long upsert(Query query, T dto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

}

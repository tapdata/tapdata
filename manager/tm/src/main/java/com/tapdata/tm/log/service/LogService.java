package com.tapdata.tm.log.service;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.log.dto.LogDto;
import com.tapdata.tm.log.entity.LogEntity;
import com.tapdata.tm.log.repository.LogRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class LogService extends BaseService<LogDto, LogEntity, ObjectId, LogRepository> {
    public LogService(@NonNull LogRepository repository) {
        super(repository, LogDto.class, LogEntity.class);
    }

    protected void beforeSave(LogDto logs, UserDetail user) {

    }

    public void save(LogDto logDto){
        LogEntity log=new LogEntity();
        BeanUtil.copyProperties(logDto,log);
        repository.getMongoOperations().save(log);
    }

}
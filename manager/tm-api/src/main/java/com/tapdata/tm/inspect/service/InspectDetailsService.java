package com.tapdata.tm.inspect.service;

import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import com.tapdata.tm.inspect.entity.InspectDetailsEntity;
import com.tapdata.tm.inspect.repository.InspectDetailsRepository;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.Map;

public interface InspectDetailsService extends IBaseService<InspectDetailsDto, InspectDetailsEntity, ObjectId, InspectDetailsRepository> {

}
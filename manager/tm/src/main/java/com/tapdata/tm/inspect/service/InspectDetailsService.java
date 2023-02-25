package com.tapdata.tm.inspect.service;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import com.tapdata.tm.inspect.entity.InspectDetailsEntity;
import com.tapdata.tm.inspect.repository.InspectDetailsRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Map;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class InspectDetailsService extends BaseService<InspectDetailsDto, InspectDetailsEntity, ObjectId, InspectDetailsRepository> {

    public InspectDetailsService(@NonNull InspectDetailsRepository repository) {
        super(repository, InspectDetailsDto.class, InspectDetailsEntity.class);
    }

    protected void beforeSave(InspectDetailsDto inspectDetails, UserDetail user) {
        inspectDetails.setTtlTime(buildTtlTime());
    }


    public static Date buildTtlTime() {
        Date setTime = new Date();
        Date ttlTime = new Date(setTime.getTime() + (6L *30*24*60*60*1000));

        try {
            String value = SettingsEnum.INSPECT_SETTING.getValue();

            if (StringUtils.isNotBlank(value)) {
                Map<String, Object> map = JsonUtil.parseJson(value, Map.class);
                Integer retentionTime = (Integer) map.get("retentionTime");
                if (retentionTime == null) {
                    retentionTime = 6;
                }
                setTime = new Date(ttlTime.getTime() - ((long) retentionTime * 30*24*60*60*1000));
                ttlTime = setTime;
            }
        } catch (Exception e) {
            ttlTime = setTime;
        }
        return ttlTime;
    }
}
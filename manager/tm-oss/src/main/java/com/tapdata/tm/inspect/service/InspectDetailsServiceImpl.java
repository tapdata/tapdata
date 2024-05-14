package com.tapdata.tm.inspect.service;

import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import com.tapdata.tm.inspect.repository.InspectDetailsRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
public class InspectDetailsServiceImpl extends InspectDetailsService {
    public InspectDetailsServiceImpl(@NonNull InspectDetailsRepository repository) {
        super(repository);
    }
    @Override
    protected void beforeSave(InspectDetailsDto dto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

}
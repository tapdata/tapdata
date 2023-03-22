package com.tapdata.tm.livedataplatform.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.livedataplatform.constant.ModeEnum;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.entity.LiveDataPlatformEntity;
import com.tapdata.tm.livedataplatform.repository.LiveDataPlatformRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;


@Service
@Slf4j
public class LiveDataPlatformService extends BaseService<LiveDataPlatformDto, LiveDataPlatformEntity, ObjectId, LiveDataPlatformRepository> {


    public LiveDataPlatformService(@NonNull LiveDataPlatformRepository liveDataPlatformRepository) {
        super(liveDataPlatformRepository, LiveDataPlatformDto.class, LiveDataPlatformEntity.class);
    }

    protected void beforeSave(LiveDataPlatformDto liveDataPlatformDto, UserDetail user) {

    }

    public Page<LiveDataPlatformDto> findData(Filter filter, UserDetail userDetail) {
        Page<LiveDataPlatformDto> liveDataPlatformDtoPage = find(filter, userDetail);
        if (liveDataPlatformDtoPage.getTotal() == 0) {
            LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
            liveDataPlatformDto.setMode(ModeEnum.INTEGRATION_PLATFORM.getValue());
            liveDataPlatformDto.setInit(Boolean.TRUE);
            save(liveDataPlatformDto, userDetail);
            liveDataPlatformDtoPage.setTotal(1);
            List<LiveDataPlatformDto> liveDataPlatformList = new ArrayList<>();
            liveDataPlatformList.add(liveDataPlatformDto);
            liveDataPlatformDtoPage.setItems(liveDataPlatformList);
        }
        return liveDataPlatformDtoPage;
    }

    public LiveDataPlatformDto findOneData(Filter filter, UserDetail userDetail) {
        LiveDataPlatformDto liveDataPlatformDto = findOne(filter, userDetail);
        if (liveDataPlatformDto == null) {
            LiveDataPlatformDto liveDataPlatform = new LiveDataPlatformDto();
            liveDataPlatform.setMode(ModeEnum.INTEGRATION_PLATFORM.getValue());
            liveDataPlatform.setInit(Boolean.TRUE);
            liveDataPlatformDto = save(liveDataPlatform, userDetail);
        }
        return liveDataPlatformDto;
    }

}
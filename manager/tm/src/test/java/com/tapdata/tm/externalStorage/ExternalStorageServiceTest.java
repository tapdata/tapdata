package com.tapdata.tm.externalStorage;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
import com.tapdata.tm.externalStorage.repository.ExternalStorageRepository;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.externalStorage.service.ExternalStorageServiceImpl;
import com.tapdata.tm.permissions.service.DataPermissionService;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


 class ExternalStorageServiceTest {


     private String actualData;

     @Test
     void findForCloudTest() {
        testFilter("cloud", true);

    }

    @Test
     void findForDaasTest() {
        testFilter("daas", false);
    }


     void testFilter(String mark, boolean cloud) {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        Filter filter = new Filter();
        Where where = new Where();
        where.and("id", mark);
        filter.setWhere(where);
        SettingsService settingsService = Mockito.mock(SettingsService.class);
        ReflectionTestUtils.setField(externalStorageService, "settingsService", settingsService);
        List<ExternalStorageEntity> list = new ArrayList<>();
        ExternalStorageEntity externalStorage = new ExternalStorageEntity();
        externalStorage.setName(mark);
        externalStorage.setUri("http://test.com");
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        BeanUtils.copyProperties(externalStorage,externalStorageDto);
        list.add(externalStorage);
        UserDetail userDetail = Mockito.mock(UserDetail.class);
        when(settingsService.isCloud()).thenReturn(cloud);
        if (cloud) {
            when(repository.findAll(filter, userDetail)).thenReturn(new ArrayList<>());
        } else {
            when(repository.findAll(filter)).thenReturn(list);
        }
        when(repository.findAll(Query.query(Criteria.where("init").is(true)
                .and("status").is(DataSourceConnectionDto.STATUS_READY)), userDetail)).thenReturn(new ArrayList<>());
        when(repository.count(where, userDetail)).thenReturn(1L);
        try (MockedStatic<DataPermissionService> data = Mockito
                .mockStatic(DataPermissionService.class)) {
            data.when(() -> DataPermissionService.isCloud()).thenReturn(cloud);
            Page<ExternalStorageDto> externalStorageDtoPage = externalStorageService.find(filter, userDetail);
            if (!cloud) {
            String actualData = externalStorageDtoPage.getItems().get(0).getName();
            assertEquals(mark, actualData);
            }else {
                assertEquals(1,externalStorageDtoPage.getTotal());
            }
        }
    }
}

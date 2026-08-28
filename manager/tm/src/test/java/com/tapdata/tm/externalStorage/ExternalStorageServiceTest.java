package com.tapdata.tm.externalStorage;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
import com.tapdata.tm.externalStorage.repository.ExternalStorageRepository;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.externalStorage.service.ExternalStorageServiceImpl;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import com.tapdata.tm.permissions.service.DataPermissionService;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


 class ExternalStorageServiceTest {

     @Test
     void findForCloudTest() {
        testFilter("cloud", true);

    }

    @Test
     void findForDaasTest() {
        testFilter("daas", false);
    }

    @Test
    void restoreMaskedSensitiveFieldsShouldRestoreAccessTokenAndMongoUri() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        ObjectId id = new ObjectId();

        ExternalStorageDto externalStorage = new ExternalStorageDto();
        externalStorage.setId(id);
        externalStorage.setType(ExternalStorageType.mongodb.name());
        externalStorage.setUri("mongodb://test:" + ExternalStorageDto.MASK_PWD + "@127.0.0.1:27017/test");
        externalStorage.setAccessToken(ExternalStorageDto.MASK_PWD);
        externalStorage.setSslCA(ExternalStorageDto.MASK_PWD);
        externalStorage.setSslKey(ExternalStorageDto.MASK_PWD);
        externalStorage.setSslPass(ExternalStorageDto.MASK_PWD);

        ExternalStorageEntity oldExternalStorage = new ExternalStorageEntity();
        oldExternalStorage.setUri("mongodb://test:password@127.0.0.1:27017/test");
        oldExternalStorage.setAccessToken("token");
        oldExternalStorage.setSslCA("ca");
        oldExternalStorage.setSslKey("key");
        oldExternalStorage.setSslPass("pass");
        when(repository.findById(id.toHexString())).thenReturn(Optional.of(oldExternalStorage));

        ReflectionTestUtils.invokeMethod(externalStorageService, "restoreMaskedSensitiveFields", externalStorage);

        assertEquals("mongodb://test:password@127.0.0.1:27017/test", externalStorage.getUri());
        assertEquals("token", externalStorage.getAccessToken());
        assertEquals("ca", externalStorage.getSslCA());
        assertEquals("key", externalStorage.getSslKey());
        assertEquals("pass", externalStorage.getSslPass());
    }

    @Test
    void restoreMaskedSensitiveFieldsShouldRestoreAccessTokenWithoutSslMask() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        ObjectId id = new ObjectId();

        ExternalStorageDto externalStorage = new ExternalStorageDto();
        externalStorage.setId(id);
        externalStorage.setAccessToken(ExternalStorageDto.MASK_PWD);

        ExternalStorageEntity oldExternalStorage = new ExternalStorageEntity();
        oldExternalStorage.setAccessToken("token");
        when(repository.findById(id.toHexString())).thenReturn(Optional.of(oldExternalStorage));

        ReflectionTestUtils.invokeMethod(externalStorageService, "restoreMaskedSensitiveFields", externalStorage);

        assertEquals("token", externalStorage.getAccessToken());
    }

    @Test
    void restoreMaskedSensitiveFieldsShouldRejectMaskedValueWhenIdIsNull() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);

        ExternalStorageDto externalStorage = new ExternalStorageDto();
        externalStorage.setSslCA(ExternalStorageDto.MASK_PWD);

        assertThrows(BizException.class,
                () -> ReflectionTestUtils.invokeMethod(externalStorageService, "restoreMaskedSensitiveFields", externalStorage));
    }

    @Test
    void restoreMaskedSensitiveFieldsShouldNotQueryRepositoryWhenNothingMasked() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);

        ExternalStorageDto externalStorage = new ExternalStorageDto();
        externalStorage.setId(new ObjectId());
        externalStorage.setSslCA("ca");
        externalStorage.setAccessToken("token");

        ReflectionTestUtils.invokeMethod(externalStorageService, "restoreMaskedSensitiveFields", externalStorage);

        Mockito.verify(repository, Mockito.never()).findById(Mockito.anyString());
    }


     void testFilter(String mark, boolean cloud) {
         new DataPermissionHelper(mock(IDataPermissionHelper.class)); //when repository.find call methods in DataPermissionHelper class this line is need
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
        externalStorage.setUri("mongodb://test:password@127.0.0.1:27017/test");
        externalStorage.setSsl(true);
        externalStorage.setSslCA("ca");
        externalStorage.setSslKey("key");
        externalStorage.setSslPass("pass");
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
                .and("status").is(DataSourceConnectionDto.STATUS_READY)))).thenReturn(new ArrayList<>());
        when(repository.count(where, userDetail)).thenReturn(1L);
        try (MockedStatic<DataPermissionService> data = Mockito
                .mockStatic(DataPermissionService.class)) {
            data.when(() -> DataPermissionService.isCloud()).thenReturn(cloud);
            Page<ExternalStorageDto> externalStorageDtoPage = externalStorageService.find(filter, userDetail);
            if (!cloud) {
                ExternalStorageDto actualData = externalStorageDtoPage.getItems().get(0);
                assertEquals(mark, actualData.getName());
                assertEquals(ExternalStorageDto.MASK_PWD, actualData.getSslCA());
                assertEquals(ExternalStorageDto.MASK_PWD, actualData.getSslKey());
                assertEquals(ExternalStorageDto.MASK_PWD, actualData.getSslPass());
                assertEquals("mongodb://test:******@127.0.0.1:27017/test", actualData.getUri());
            }else {
                assertEquals(1,externalStorageDtoPage.getTotal());
            }
        }
    }
}

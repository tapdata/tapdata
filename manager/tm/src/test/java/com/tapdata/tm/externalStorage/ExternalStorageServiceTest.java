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
import com.tapdata.tm.utils.AES256Util;
import com.mongodb.client.result.UpdateResult;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;


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
        UserDetail userDetail = Mockito.mock(UserDetail.class);
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
        when(repository.findById(id, userDetail)).thenReturn(Optional.of(oldExternalStorage));

        ReflectionTestUtils.invokeMethod(externalStorageService, "restoreMaskedSensitiveFields", externalStorage, userDetail);

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
        UserDetail userDetail = Mockito.mock(UserDetail.class);
        ObjectId id = new ObjectId();

        ExternalStorageDto externalStorage = new ExternalStorageDto();
        externalStorage.setId(id);
        externalStorage.setAccessToken(ExternalStorageDto.MASK_PWD);

        ExternalStorageEntity oldExternalStorage = new ExternalStorageEntity();
        oldExternalStorage.setAccessToken("token");
        when(repository.findById(id, userDetail)).thenReturn(Optional.of(oldExternalStorage));

        ReflectionTestUtils.invokeMethod(externalStorageService, "restoreMaskedSensitiveFields", externalStorage, userDetail);

        assertEquals("token", externalStorage.getAccessToken());
    }

    @Test
    void restoreMaskedSensitiveFieldsShouldRejectMaskedValueWhenIdIsNull() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        UserDetail userDetail = Mockito.mock(UserDetail.class);

        ExternalStorageDto externalStorage = new ExternalStorageDto();
        externalStorage.setSslCA(ExternalStorageDto.MASK_PWD);

        assertThrows(BizException.class,
                () -> ReflectionTestUtils.invokeMethod(externalStorageService, "restoreMaskedSensitiveFields", externalStorage, userDetail));
    }

    @Test
    void restoreMaskedSensitiveFieldsShouldNotQueryRepositoryWhenNothingMasked() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        UserDetail userDetail = Mockito.mock(UserDetail.class);

        ExternalStorageDto externalStorage = new ExternalStorageDto();
        externalStorage.setId(new ObjectId());
        externalStorage.setSslCA("ca");
        externalStorage.setAccessToken("token");

        ReflectionTestUtils.invokeMethod(externalStorageService, "restoreMaskedSensitiveFields", externalStorage, userDetail);

        Mockito.verify(repository, Mockito.never()).findById(Mockito.any(ObjectId.class), Mockito.any(UserDetail.class));
    }

    @Test
    void saveShouldMaskMongoUriBeforeReturn() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = Mockito.spy(new ExternalStorageServiceImpl(repository));
        UserDetail userDetail = Mockito.mock(UserDetail.class);
        SettingsService settingsService = Mockito.mock(SettingsService.class);
        ObjectId id = new ObjectId();
        ReflectionTestUtils.setField(externalStorageService, "settingsService", settingsService);
        when(settingsService.isCloud()).thenReturn(true);

        ExternalStorageDto dto = new ExternalStorageDto();
        dto.setType(ExternalStorageType.mongodb.name());
        dto.setName("external-storage");
        dto.setUri("mongodb://admin:password@127.0.0.1:27017/test");

        doNothing().when(externalStorageService).sendTestConnection(any(ExternalStorageDto.class), any(UserDetail.class));
        when(repository.findOne(any(Query.class))).thenReturn(Optional.empty());
        when(repository.save(any(ExternalStorageEntity.class), eq(userDetail))).thenAnswer(invocation -> {
            ExternalStorageEntity entity = invocation.getArgument(0);
            entity.setId(id);
            return entity;
        });
        when(repository.findById(any(ObjectId.class), eq(userDetail))).thenAnswer(invocation -> {
            ExternalStorageEntity entity = new ExternalStorageEntity();
            entity.setId(id);
            entity.setType(ExternalStorageType.mongodb.name());
            entity.setUri(AES256Util.Aes256Encode("mongodb://admin:password@127.0.0.1:27017/test"));
            return Optional.of(entity);
        });

        ExternalStorageDto saved = externalStorageService.save(dto, userDetail);

        assertEquals("mongodb://admin:******@127.0.0.1:27017/test", saved.getUri());
        assertTrue(!saved.getUri().contains("password"));
    }

    @Test
    void updateByWhereShouldRestoreMaskedFieldsBeforeWriting() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        UserDetail userDetail = Mockito.mock(UserDetail.class);

        ExternalStorageEntity oldExternalStorage = new ExternalStorageEntity();
        oldExternalStorage.setUri("mongodb://admin:password@127.0.0.1:27017/test");
        oldExternalStorage.setAccessToken("token");
        oldExternalStorage.setSslCA("ca");
        oldExternalStorage.setSslKey("key");
        oldExternalStorage.setSslPass("pass");
        when(repository.findOne(any(Where.class), eq(userDetail))).thenReturn(Optional.of(oldExternalStorage));
        when(repository.findOne(any(Query.class))).thenReturn(Optional.empty());
        when(repository.filterToQuery(any(Filter.class))).thenReturn(new Query());
        when(repository.count(any(Where.class), eq(userDetail))).thenReturn(1L);

        ExternalStorageDto dto = new ExternalStorageDto();
        dto.setName("external-storage");
        dto.setType(ExternalStorageType.mongodb.name());
        dto.setUri("mongodb://admin:******@127.0.0.1:27017/test");
        dto.setAccessToken(ExternalStorageDto.MASK_PWD);
        dto.setSslCA(ExternalStorageDto.MASK_PWD);
        dto.setSslKey(ExternalStorageDto.MASK_PWD);
        dto.setSslPass(ExternalStorageDto.MASK_PWD);

        UpdateResult updateResult = Mockito.mock(UpdateResult.class);
        when(updateResult.getModifiedCount()).thenReturn(1L);
        when(repository.updateByWhere(any(Query.class), any(ExternalStorageEntity.class), eq(userDetail))).thenReturn(updateResult);

        long count = externalStorageService.updateByWhere(new Where(), dto, userDetail);

        assertEquals(1L, count);
        ArgumentCaptor<ExternalStorageEntity> entityCaptor = ArgumentCaptor.forClass(ExternalStorageEntity.class);
        Mockito.verify(repository).updateByWhere(any(Query.class), entityCaptor.capture(), eq(userDetail));
        ExternalStorageEntity entity = entityCaptor.getValue();
        assertEquals("mongodb://admin:password@127.0.0.1:27017/test", AES256Util.Aes256Decode(entity.getUri()));
        assertEquals("token", entity.getAccessToken());
        assertEquals("ca", entity.getSslCA());
        assertEquals("key", entity.getSslKey());
        assertEquals("pass", entity.getSslPass());
    }

    @Test
    void upsertByWhereShouldRestoreMaskedFieldsBeforeWriting() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        UserDetail userDetail = Mockito.mock(UserDetail.class);

        ExternalStorageEntity oldExternalStorage = new ExternalStorageEntity();
        oldExternalStorage.setUri("mongodb://admin:password@127.0.0.1:27017/test");
        oldExternalStorage.setAccessToken("token");
        oldExternalStorage.setSslCA("ca");
        oldExternalStorage.setSslKey("key");
        oldExternalStorage.setSslPass("pass");
        oldExternalStorage.setName("external-storage");
        when(repository.findOne(any(Where.class), eq(userDetail))).thenReturn(Optional.of(oldExternalStorage));
        when(repository.findOne(any(Query.class))).thenReturn(Optional.empty());
        when(repository.filterToQuery(any(Filter.class))).thenReturn(new Query());
        when(repository.count(any(Where.class), eq(userDetail))).thenReturn(1L);
        when(repository.upsert(any(Query.class), any(ExternalStorageEntity.class), eq(userDetail))).thenReturn(1L);

        ExternalStorageDto dto = new ExternalStorageDto();
        dto.setName("external-storage");
        dto.setType(ExternalStorageType.mongodb.name());
        dto.setUri("mongodb://admin:******@127.0.0.1:27017/test");
        dto.setAccessToken(ExternalStorageDto.MASK_PWD);
        dto.setSslCA(ExternalStorageDto.MASK_PWD);
        dto.setSslKey(ExternalStorageDto.MASK_PWD);
        dto.setSslPass(ExternalStorageDto.MASK_PWD);

        ExternalStorageDto result = externalStorageService.upsertByWhere(new Where(), dto, userDetail);

        ArgumentCaptor<ExternalStorageEntity> entityCaptor = ArgumentCaptor.forClass(ExternalStorageEntity.class);
        Mockito.verify(repository).upsert(any(Query.class), entityCaptor.capture(), eq(userDetail));
        ExternalStorageEntity entity = entityCaptor.getValue();
        assertEquals("mongodb://admin:password@127.0.0.1:27017/test", AES256Util.Aes256Decode(entity.getUri()));
        assertEquals("token", entity.getAccessToken());
        assertEquals("ca", entity.getSslCA());
        assertEquals("key", entity.getSslKey());
        assertEquals("pass", entity.getSslPass());
        assertEquals("mongodb://admin:******@127.0.0.1:27017/test", result.getUri());
        assertEquals(ExternalStorageDto.MASK_PWD, result.getAccessToken());
    }

    @Test
    void updateByWhereDocumentShouldRestoreMaskedFieldsAndEncryptUriBeforeWriting() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        UserDetail userDetail = Mockito.mock(UserDetail.class);

        ExternalStorageEntity oldExternalStorage = new ExternalStorageEntity();
        oldExternalStorage.setUri("mongodb://admin:password@127.0.0.1:27017/test");
        oldExternalStorage.setAccessToken("token");
        oldExternalStorage.setSslCA("ca");
        oldExternalStorage.setSslKey("key");
        oldExternalStorage.setSslPass("pass");
        when(repository.findOne(any(Where.class), eq(userDetail))).thenReturn(Optional.of(oldExternalStorage));
        when(repository.filterToQuery(any(Filter.class))).thenReturn(new Query());
        when(repository.count(any(Where.class), eq(userDetail))).thenReturn(1L);
        UpdateResult updateResult = Mockito.mock(UpdateResult.class);
        when(updateResult.getModifiedCount()).thenReturn(1L);
        when(repository.update(any(Query.class), any(Update.class), eq(userDetail))).thenReturn(updateResult);

        Document setDoc = new Document()
                .append("type", ExternalStorageType.mongodb.name())
                .append("uri", "mongodb://admin:******@127.0.0.1:27017/test")
                .append("accessToken", ExternalStorageDto.MASK_PWD)
                .append("sslCA", ExternalStorageDto.MASK_PWD)
                .append("sslKey", ExternalStorageDto.MASK_PWD)
                .append("sslPass", ExternalStorageDto.MASK_PWD);
        Document updateDoc = new Document("$set", setDoc);

        long count = externalStorageService.updateByWhere(new Where(), updateDoc, userDetail);

        assertEquals(1L, count);
        assertEquals("mongodb://admin:password@127.0.0.1:27017/test", AES256Util.Aes256Decode(setDoc.getString("uri")));
        assertEquals("token", setDoc.getString("accessToken"));
        assertEquals("ca", setDoc.getString("sslCA"));
        assertEquals("key", setDoc.getString("sslKey"));
        assertEquals("pass", setDoc.getString("sslPass"));
    }

    @Test
    void updateByWhereShouldRejectWhenWhereMatchesMultipleRecords() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        UserDetail userDetail = Mockito.mock(UserDetail.class);

        when(repository.count(any(Where.class), eq(userDetail))).thenReturn(2L);

        ExternalStorageDto dto = new ExternalStorageDto();
        dto.setName("external-storage");
        dto.setType(ExternalStorageType.mongodb.name());
        dto.setUri("mongodb://admin:******@127.0.0.1:27017/test");

        assertThrows(BizException.class,
                () -> externalStorageService.updateByWhere(new Where().and("type", ExternalStorageType.mongodb.name()), dto, userDetail));
        Mockito.verify(repository, Mockito.never()).updateByWhere(any(Query.class), any(ExternalStorageEntity.class), eq(userDetail));
    }

    @Test
    void updateByWhereDocumentShouldRejectWhenWhereMatchesMultipleRecords() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        UserDetail userDetail = Mockito.mock(UserDetail.class);

        when(repository.count(any(Where.class), eq(userDetail))).thenReturn(2L);

        Document setDoc = new Document("uri", "mongodb://admin:******@127.0.0.1:27017/test");
        Document updateDoc = new Document("$set", setDoc);

        assertThrows(BizException.class,
                () -> externalStorageService.updateByWhere(new Where(), updateDoc, userDetail));
        Mockito.verify(repository, Mockito.never()).update(any(Query.class), any(Update.class), eq(userDetail));
    }

    @Test
    void hasMaskedSensitiveFieldsShouldNotTreatQueryStringAsterisksAsMaskedUri() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);

        ExternalStorageDto dto = new ExternalStorageDto();
        dto.setType(ExternalStorageType.mongodb.name());
        dto.setUri("mongodb://admin:password@127.0.0.1:27017/tapdata?appName=share******");

        Boolean masked = ReflectionTestUtils.invokeMethod(externalStorageService, "hasMaskedSensitiveFields", dto);
        assertEquals(Boolean.FALSE, masked);
    }

    @Test
    void updateByWhereDocumentShouldNotFailWhenSetValuesAreNotString() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        UserDetail userDetail = Mockito.mock(UserDetail.class);

        when(repository.filterToQuery(any(Filter.class))).thenReturn(new Query());
        UpdateResult updateResult = Mockito.mock(UpdateResult.class);
        when(updateResult.getModifiedCount()).thenReturn(1L);
        when(repository.update(any(Query.class), any(Update.class), eq(userDetail))).thenReturn(updateResult);

        Document setDoc = new Document()
                .append("type", ExternalStorageType.mongodb.name())
                .append("uri", 123)
                .append("ttlDay", 7);
        Document updateDoc = new Document("$set", setDoc);

        long count = externalStorageService.updateByWhere(new Where(), updateDoc, userDetail);

        assertEquals(1L, count);
        Mockito.verify(repository).update(any(Query.class), any(Update.class), eq(userDetail));
    }

    @Test
    void updateByWhereDocumentShouldRejectWhenEncryptFails() {
        ExternalStorageRepository repository = Mockito.mock(ExternalStorageRepository.class);
        ExternalStorageServiceImpl externalStorageService = new ExternalStorageServiceImpl(repository);
        UserDetail userDetail = Mockito.mock(UserDetail.class);

        ExternalStorageEntity oldExternalStorage = new ExternalStorageEntity();
        oldExternalStorage.setUri("mongodb://admin:password@127.0.0.1:27017/test");
        when(repository.count(any(Where.class), eq(userDetail))).thenReturn(1L);
        when(repository.findOne(any(Where.class), eq(userDetail))).thenReturn(Optional.of(oldExternalStorage));

        Document setDoc = new Document("uri", "mongodb://admin:******@127.0.0.1:27017/test");
        Document updateDoc = new Document("$set", setDoc);

        try (MockedStatic<AES256Util> aes = Mockito.mockStatic(AES256Util.class)) {
            aes.when(() -> AES256Util.Aes256Decode(Mockito.anyString())).thenAnswer(invocation -> invocation.getArgument(0));
            aes.when(() -> AES256Util.Aes256Encode(Mockito.anyString())).thenThrow(new RuntimeException("encrypt failed"));
            assertThrows(BizException.class,
                    () -> externalStorageService.updateByWhere(new Where(), updateDoc, userDetail));
        }
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

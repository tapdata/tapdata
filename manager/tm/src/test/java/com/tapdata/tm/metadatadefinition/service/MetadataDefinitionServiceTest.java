package com.tapdata.tm.metadatadefinition.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.entity.MetadataDefinitionEntity;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.repository.MetadataDefinitionRepository;
import com.tapdata.tm.utils.Lists;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class MetadataDefinitionServiceTest {
    MetadataDefinitionService metadataDefinitionService;
    MongoTemplate mongoTemplate;

    @BeforeEach
    void init() {
        metadataDefinitionService = mock(MetadataDefinitionService.class);
        mongoTemplate = mock(MongoTemplate.class);
        ReflectionTestUtils.setField(metadataDefinitionService, "mongoTemplate", mongoTemplate);
    }

    @Nested
    class BatchPushListTagsTest {
        BatchUpdateParam batchUpdateParam;
        @BeforeEach
        void init() {
            batchUpdateParam = mock(BatchUpdateParam.class);

            when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), any(Class.class))).thenReturn(mock(UpdateResult.class));
            when(batchUpdateParam.getId()).thenReturn(Lists.newArrayList());
            when(batchUpdateParam.getListtags()).thenReturn(Lists.newArrayList());
        }

        void doVerify(String tableName, int times) {
            when(metadataDefinitionService.batchPushListTags(tableName, batchUpdateParam)).thenCallRealMethod();
            metadataDefinitionService.batchPushListTags(tableName, batchUpdateParam);

            verify(batchUpdateParam, times(1)).getId();
            verify(batchUpdateParam, times(1)).getListtags();
            verify(mongoTemplate, times(times)).updateMulti(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void testConnections() {
            doVerify("Connections", 1);
        }

        @Test
        void testTask() {
            doVerify("Task", 1);
        }

        @Test
        void testModules() {
            doVerify("Modules", 1);
        }

        @Test
        void testOther() {
            doVerify("", 0);
        }
    }
    @Nested
    class FindTest{
        SettingsService settingsService;
        MetadataDefinitionRepository metadataDefinitionRepository;
        MetadataDefinitionService metadataDefinitionService;
        @BeforeEach
        void init() {
            settingsService = mock(SettingsService.class);
            metadataDefinitionRepository = mock(MetadataDefinitionRepository.class);
            metadataDefinitionService = new MetadataDefinitionService(metadataDefinitionRepository);
            ReflectionTestUtils.setField(metadataDefinitionService, "settingsService", settingsService);
        }
        @Test
        void testCloud() {
            when(settingsService.isCloud()).thenReturn(true);
            Filter filter = new Filter();
            metadataDefinitionService.find(filter,mock(UserDetail.class));
            verify(metadataDefinitionRepository,times(1)).findAll(any(Filter.class),any(UserDetail.class));
        }

        @Test
        void testDass() {
            when(settingsService.isCloud()).thenReturn(false);
            Filter filter = new Filter();
            metadataDefinitionService.find(filter,mock(UserDetail.class));
            verify(metadataDefinitionRepository,times(1)).findAll(any(Filter.class));
        }
    }

    @Nested
    class deleteByIdTest {
        @Test
        @DisplayName("test for metadataDefinitionDto is null")
        void test1() {
            ObjectId id = mock(ObjectId.class);
            UserDetail user = mock(UserDetail.class);
            when(metadataDefinitionService.findById(id, user)).thenReturn(null);
            doCallRealMethod().when(metadataDefinitionService).deleteById(id, user);
            assertThrows(BizException.class, () -> metadataDefinitionService.deleteById(id, user));
        }
    }
    @Nested
    class saveTest {
        MetadataDefinitionDto metadataDefinitionDto;
        UserDetail userDetail;
        MetadataDefinitionRepository repository;
        @BeforeEach
        void beforeEach() {
            metadataDefinitionDto = mock(MetadataDefinitionDto.class);
            userDetail = mock(UserDetail.class);
            repository = mock(MetadataDefinitionRepository.class);
            ReflectionTestUtils.setField(metadataDefinitionService, "repository", repository);
            doCallRealMethod().when(metadataDefinitionService).save(metadataDefinitionDto, userDetail);
        }
        @Test
        @DisplayName("test save when user is admin and parent is null")
        void test1() {
            ObjectId id = mock(ObjectId.class);
            when(metadataDefinitionDto.getId()).thenReturn(id);
            when(userDetail.isRoot()).thenReturn(true);
            when(metadataDefinitionService.findById(id, userDetail)).thenReturn(metadataDefinitionDto);
            String parentId = "671b045dfdd7620f87d3d1a6";
            when(metadataDefinitionDto.getParent_id()).thenReturn(parentId);
            assertThrows(BizException.class, ()->metadataDefinitionService.save(metadataDefinitionDto, userDetail));
        }
        @Test
        @DisplayName("test save when user is admin and id is null")
        void test2() {
            when(metadataDefinitionDto.getId()).thenReturn(null);
            when(userDetail.isRoot()).thenReturn(true);
            String parentId = "671b045dfdd7620f87d3d1a6";
            when(metadataDefinitionDto.getParent_id()).thenReturn(parentId);
            when(metadataDefinitionService.findById(any(ObjectId.class), any(UserDetail.class))).thenReturn(metadataDefinitionDto);
            when(repository.save(eq(null), any(UserDetail.class))).thenReturn(mock(MetadataDefinitionEntity.class));
            MetadataDefinitionDto actual = metadataDefinitionService.save(metadataDefinitionDto, userDetail);
            assertNotNull(actual);
        }
        @Test
        @DisplayName("test save when user is admin and exsitedOne is null")
        void test3() {
            when(metadataDefinitionDto.getId()).thenReturn(mock(ObjectId.class));
            when(userDetail.isRoot()).thenReturn(true);
            when(metadataDefinitionService.findById(any(ObjectId.class), any(UserDetail.class))).thenReturn(null);
            assertThrows(BizException.class, ()->metadataDefinitionService.save(metadataDefinitionDto, userDetail));
        }
    }

}

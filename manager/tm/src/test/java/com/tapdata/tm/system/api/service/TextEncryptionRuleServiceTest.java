package com.tapdata.tm.system.api.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.modules.vo.ModulesDetailVo;
import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.system.api.entity.TextEncryptionRuleEntity;
import com.tapdata.tm.system.api.enums.OutputType;
import com.tapdata.tm.system.api.repository.TextEncryptionRuleRepository;
import com.tapdata.tm.utils.Lists;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TextEncryptionRuleServiceTest {

    TextEncryptionRuleService service;
    TextEncryptionRuleRepository repository;
    ModulesService modulesService;
    SettingsService settingsService;

    @BeforeEach
    void init() {
        service = mock(TextEncryptionRuleService.class);
        repository = mock(TextEncryptionRuleRepository.class);
        modulesService = mock(ModulesService.class);
        settingsService = mock(SettingsService.class);
        ReflectionTestUtils.setField(service, "repository", repository);
        ReflectionTestUtils.setField(service, "modulesService", modulesService);
        ReflectionTestUtils.setField(service, "settingsService", settingsService);
    }

    @Nested
    class checkAudioSwitchStatusTest {
        @Test
        void testNormal() {
            List<Settings> settings = new ArrayList<>();
            Settings set = new Settings();
            settings.add(set);
            set.setValue("true");
            when(service.checkAudioSwitchStatus()).thenCallRealMethod();
            when(settingsService.findAll(any(Query.class))).thenReturn(settings);
            Assertions.assertTrue(service.checkAudioSwitchStatus());
        }

        @Test
        void testEmpty() {
            List<Settings> settings = new ArrayList<>();
            when(service.checkAudioSwitchStatus()).thenCallRealMethod();
            when(settingsService.findAll(any(Query.class))).thenReturn(settings);
            Assertions.assertFalse(service.checkAudioSwitchStatus());
        }

        @Test
        void testFalse() {
            List<Settings> settings = new ArrayList<>();
            Settings set = new Settings();
            settings.add(set);
            set.setValue("false");
            when(service.checkAudioSwitchStatus()).thenCallRealMethod();
            when(settingsService.findAll(any(Query.class))).thenReturn(settings);
            Assertions.assertFalse(service.checkAudioSwitchStatus());
        }

    }


    @Nested
    class getByIdTest {
        @BeforeEach
        void init() {
            when(service.getById(anyString())).thenCallRealMethod();
            when(service.getById(any(Collection.class))).thenCallRealMethod();
        }

        @Test
        void testEmpty() {
            List<TextEncryptionRuleEntity> findResult = new ArrayList<>();
            when(repository.findAll(any(Query.class))).thenReturn(findResult);
            List<TextEncryptionRuleDto> ruleDtos = service.getById("");
            Assertions.assertTrue(ruleDtos.isEmpty());
        }

        @Test
        void testEmptyObjectId() {
            List<TextEncryptionRuleEntity> findResult = new ArrayList<>();
            when(repository.findAll(any(Query.class))).thenReturn(findResult);
            List<TextEncryptionRuleDto> ruleDtos = service.getById("id");
            Assertions.assertTrue(ruleDtos.isEmpty());
        }

        @Test
        void testNormal() {
            List<TextEncryptionRuleEntity> findResult = new ArrayList<>();
            when(repository.findAll(any(Query.class))).thenReturn(findResult);
            findResult.add(null);
            findResult.add(new TextEncryptionRuleEntity());
            List<TextEncryptionRuleDto> ruleDtos = service.getById(new ObjectId().toHexString() + ",id");
            Assertions.assertFalse(ruleDtos.isEmpty());
            Assertions.assertEquals(1, ruleDtos.size());
        }
    }

    @Nested
    class pageTest {
        @BeforeEach
        void init() {
            when(service.page(any(Filter.class))).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Filter filter = new Filter();
            when(repository.count(any(Query.class))).thenReturn(1L);
            when(repository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            Assertions.assertEquals(0, service.page(filter).getItems().size());
        }

        @Test
        void testEmpty() {
            Filter filter = new Filter();
            when(repository.count(any(Query.class))).thenReturn(0L);
            Assertions.assertEquals(0, service.page(filter).getItems().size());
        }

        @Test
        void teseType() {
            Filter filter = new Filter();
            filter.getWhere().put("type", "1");
            when(repository.count(any(Query.class))).thenReturn(0L);
            Assertions.assertEquals(0, service.page(filter).getItems().size());
        }

        @Test
        void teseTypeNotNumber() {
            Filter filter = new Filter();
            filter.getWhere().put("type", "xxx");
            when(repository.count(any(Query.class))).thenReturn(0L);
            Assertions.assertEquals(0, service.page(filter).getItems().size());
        }
    }

    @Nested
    class createTest {
        TextEncryptionRuleDto dto;
        UserDetail userDetail;

        @BeforeEach
        void init() {
            dto = new TextEncryptionRuleDto();
            userDetail = mock(UserDetail.class);
            when(service.create(dto, userDetail)).thenCallRealMethod();
            doCallRealMethod().when(service).verifyFormData(dto);
            when(service.mapToDto(any(TextEncryptionRuleEntity.class))).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            dto.setName("gavin'xiao");
            dto.setRegex(".*");
            dto.setOutputType(OutputType.CUSTOM.getCode());
            dto.setOutputCount(-1);
            when(repository.count(any(Query.class))).thenReturn(0L);
            when(repository.save(any(TextEncryptionRuleEntity.class), any(UserDetail.class))).thenReturn(new TextEncryptionRuleEntity());
            Assertions.assertTrue(service.create(dto, userDetail));
        }

        @Test
        void testNameEmpty() {
            dto.setName("");
            dto.setRegex(".*");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    service.create(dto, userDetail);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "api.encryption.name.empty");
                    throw e;
                }
            });
        }

        @Test
        void testRegexEmpty() {
            dto.setName("gavin'xiao");
            dto.setRegex("");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    service.create(dto, userDetail);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "api.encryption.regex.empty");
                    throw e;
                }
            });
        }

        @Test
        void testNameInvalid() {
            dto.setName("gavin'xiao");
            dto.setRegex(".*");
            dto.setOutputType(OutputType.CUSTOM.getCode());
            dto.setOutputCount(1);
            when(repository.count(any(Query.class))).thenReturn(1L);
            when(repository.save(any(TextEncryptionRuleEntity.class), any(UserDetail.class))).thenReturn(new TextEncryptionRuleEntity());
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    service.create(dto, userDetail);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "api.encryption.name.invalid");
                    throw e;
                }
            });
        }

        @Test
        void testNameTooLong() {
            dto.setName(genericChars(31));
            dto.setRegex(".*");
            when(repository.count(any(Query.class))).thenReturn(0L);
            when(repository.save(any(TextEncryptionRuleEntity.class), any(UserDetail.class))).thenReturn(new TextEncryptionRuleEntity());
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    service.create(dto, userDetail);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "api.encryption.name.too.long");
                    throw e;
                }
            });
        }

        @Test
        void testRegexTooLong() {
            dto.setName("gavin'xiao");
            dto.setRegex(genericChars(513));
            when(repository.count(any(Query.class))).thenReturn(0L);
            when(repository.save(any(TextEncryptionRuleEntity.class), any(UserDetail.class))).thenReturn(new TextEncryptionRuleEntity());
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    service.create(dto, userDetail);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "api.encryption.regex.too.long");
                    throw e;
                }
            });
        }

        @Test
        void testDescriptionTooLong() {
            dto.setName("gavin'xiao");
            dto.setRegex(".*gavin'xiao");
            dto.setDescription(genericChars(513));
            when(repository.count(any(Query.class))).thenReturn(0L);
            when(repository.save(any(TextEncryptionRuleEntity.class), any(UserDetail.class))).thenReturn(new TextEncryptionRuleEntity());
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    service.create(dto, userDetail);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "api.encryption.description.too.long");
                    throw e;
                }
            });
        }
    }

    @Nested
    class updateTest {
        TextEncryptionRuleDto dto;
        UserDetail userDetail;
        UpdateResult updateResult;

        @BeforeEach
        void init() {
            updateResult = mock(UpdateResult.class);
            when(updateResult.getModifiedCount()).thenReturn(1L);
            when(updateResult.getMatchedCount()).thenReturn(1L);
            dto = new TextEncryptionRuleDto();
            dto.setDescription("gavin'xiao");
            userDetail = mock(UserDetail.class);
            when(service.update(dto, userDetail)).thenCallRealMethod();
            doCallRealMethod().when(service).verifyFormData(dto);
            when(service.mapToDto(any(TextEncryptionRuleEntity.class))).thenCallRealMethod();
            when(repository.update(any(Query.class), any(TextEncryptionRuleEntity.class))).thenReturn(updateResult);
        }

        @Test
        void normal() {
            dto.setId(new ObjectId());
            when(repository.count(any(Query.class))).thenReturn(0L);
            Assertions.assertDoesNotThrow(() -> service.update(dto, userDetail));
            verify(repository, times(1)).update(any(Query.class), any(TextEncryptionRuleEntity.class));
            verify(repository, times(0)).count(any(Query.class));
        }

        @Test
        void testUpdateName() {
            dto.setId(new ObjectId());
            dto.setName("gavin'xiao");
            when(repository.count(any(Query.class))).thenReturn(0L);
            Assertions.assertDoesNotThrow(() -> service.update(dto, userDetail));
            verify(repository, times(1)).update(any(Query.class), any(TextEncryptionRuleEntity.class));
            verify(repository, times(1)).count(any(Query.class));
        }

        @Test
        void testUpdateNameInvalid() {
            dto.setId(new ObjectId());
            dto.setName("gavin'xiao");
            when(repository.count(any(Query.class))).thenReturn(1L);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    service.update(dto, userDetail);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "api.encryption.name.invalid");
                    throw e;
                }
            });
            verify(repository, times(0)).update(any(Query.class), any(TextEncryptionRuleEntity.class));
            verify(repository, times(1)).count(any(Query.class));
        }

        @Test
        void testIdIsEmpty() {
            dto.setId(null);
            when(repository.count(any(Query.class))).thenReturn(0L);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    service.update(dto, userDetail);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "api.encryption.id.empty");
                    throw e;
                }
            });
            verify(repository, times(0)).update(any(Query.class), any(TextEncryptionRuleEntity.class));
            verify(repository, times(0)).count(any(Query.class));
        }
    }

    @Nested
    class batchDeleteTest {
        UserDetail userDetail;
        UpdateResult updateResult;

        @BeforeEach
        void init() {
            updateResult = mock(UpdateResult.class);
            when(updateResult.getModifiedCount()).thenReturn(1L);
            when(updateResult.getMatchedCount()).thenReturn(1L);
            userDetail = mock(UserDetail.class);
            when(repository.update(any(Query.class), any(Update.class), any(UserDetail.class))).thenReturn(updateResult);
        }

        @Test
        void testNormal() {
            String hexString = new ObjectId().toHexString();
            when(service.batchDelete(hexString, userDetail)).thenCallRealMethod();
            Assertions.assertTrue(service.batchDelete(hexString, userDetail));
            verify(repository, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));
        }

        @Test
        void testEmpty() {
            when(service.batchDelete(null, userDetail)).thenCallRealMethod();
            Assertions.assertFalse(service.batchDelete(null, userDetail));
            verify(repository, times(0)).update(any(Query.class), any(Update.class), any(UserDetail.class));
        }

        @Test
        void testSplitEmpty() {
            when(service.batchDelete(",", userDetail)).thenCallRealMethod();
            Assertions.assertFalse(service.batchDelete(",", userDetail));
            verify(repository, times(0)).update(any(Query.class), any(Update.class), any(UserDetail.class));
        }

        @Test
        void testIdEmpty() {
            when(service.batchDelete("id,ids", userDetail)).thenCallRealMethod();
            Assertions.assertFalse(service.batchDelete("id,ids", userDetail));
            verify(repository, times(0)).update(any(Query.class), any(Update.class), any(UserDetail.class));
        }
    }

    @Nested
    class getFieldEncryptionRuleByApiIdTest {

        @Test
        void testNormal() {
            List<Field> fields = new ArrayList();
            Path path = mock(Path.class);
            when(path.getFields()).thenReturn(fields);
            fields.add(null);
            Field f1 = new Field();
            f1.setFieldName("name");
            f1.setTextEncryptionRuleIds(Lists.newArrayList("id1", "id2"));
            Field f2 = new Field();
            f2.setTextEncryptionRuleIds(new ArrayList<>());
            fields.add(f1);
            fields.add(f2);
            when(service.toRule(anyMap(), anySet())).thenReturn(new HashMap<>());
            when(service.getFieldEncryptionRuleByApiId(anyString())).thenCallRealMethod();
            when(service.getPartByApiId(anyString())).thenReturn(path);
            Map<String, List<TextEncryptionRuleDto>> result = service.getFieldEncryptionRuleByApiId("");
            Assertions.assertTrue(result.isEmpty());
        }

        @Test
        void testPathIsNull() {
            when(service.getPartByApiId(anyString())).thenReturn(null);
            when(service.getFieldEncryptionRuleByApiId(anyString())).thenCallRealMethod();
            Map<String, List<TextEncryptionRuleDto>> result = service.getFieldEncryptionRuleByApiId("");
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @Nested
    class getPartByApiIdTest {

        @Test
        void testNullPath() {
            when(modulesService.findById(anyString())).thenReturn(null);
            when(service.getPartByApiId(anyString())).thenCallRealMethod();
            Assertions.assertNull(service.getPartByApiId(""));
        }

        @Test
        void testNormal() {
            ModulesDetailVo vo = new ModulesDetailVo();
            vo.setPaths(new ArrayList<>());
            vo.getPaths().add(new Path());
            when(modulesService.findById(anyString())).thenReturn(vo);
            when(service.getPartByApiId(anyString())).thenCallRealMethod();
            Assertions.assertNotNull(service.getPartByApiId(""));
        }

        @Test
        void testEmptyPaths() {
            ModulesDetailVo vo = new ModulesDetailVo();
            vo.setPaths(new ArrayList<>());
            when(modulesService.findById(anyString())).thenReturn(vo);
            when(service.getPartByApiId(anyString())).thenCallRealMethod();
            Assertions.assertNull(service.getPartByApiId(""));
        }
    }

    @Nested
    class toRuleTest {
        @Test
        void testNormal() {
            Set<String> ruleIds = new HashSet<>();
            Map<String, List<String>> fieldRuleIds = new HashMap<>();
            List<TextEncryptionRuleDto> rules = new ArrayList<>();
            when(service.toRule(anyMap(), anySet())).thenCallRealMethod();
            when(service.getById(anySet())).thenReturn(rules);
            TextEncryptionRuleDto rule = new TextEncryptionRuleDto();
            rule.setId(new ObjectId());
            rules.add(rule);
            ruleIds.add(rule.getId().toHexString());
            fieldRuleIds.put("name", Lists.newArrayList(rule.getId().toHexString()));

            Map<String, List<TextEncryptionRuleDto>> result = service.toRule(fieldRuleIds, ruleIds);
            Assertions.assertFalse(result.isEmpty());
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(1, result.get("name").size());
        }

        @Test
        void testNotFindAnyRule() {
            Set<String> ruleIds = new HashSet<>();
            Map<String, List<String>> fieldRuleIds = new HashMap<>();
            when(service.toRule(anyMap(), anySet())).thenCallRealMethod();
            when(service.getById(anySet())).thenReturn(new ArrayList<>());
            TextEncryptionRuleDto rule = new TextEncryptionRuleDto();
            rule.setId(new ObjectId());
            ruleIds.add(rule.getId().toHexString());
            fieldRuleIds.put("name", Lists.newArrayList(rule.getId().toHexString()));

            Map<String, List<TextEncryptionRuleDto>> result = service.toRule(fieldRuleIds, ruleIds);
            Assertions.assertTrue(result.isEmpty());
        }

        @Test
        void testRuleMapEmpty() {
            List<TextEncryptionRuleDto> rules = new ArrayList<>();
            Set<String> ruleIds = new HashSet<>();
            when(service.toRule(anyMap(), anySet())).thenCallRealMethod();
            when(service.getById(anySet())).thenReturn(rules);
            TextEncryptionRuleDto rule = new TextEncryptionRuleDto();
            rules.add(rule);
            rule.setId(new ObjectId());
            ruleIds.add(rule.getId().toHexString());

            Map<String, List<TextEncryptionRuleDto>> result = service.toRule(new HashMap<>(), ruleIds);
            Assertions.assertTrue(result.isEmpty());
        }

        @Test
        void testRuleIdEmpty() {
            List<TextEncryptionRuleDto> rules = new ArrayList<>();
            Map<String, List<String>> fieldRuleIds = new HashMap<>();
            when(service.toRule(anyMap(), anySet())).thenCallRealMethod();
            when(service.getById(anySet())).thenReturn(rules);
            TextEncryptionRuleDto rule = new TextEncryptionRuleDto();
            rules.add(rule);
            rule.setId(new ObjectId());
            fieldRuleIds.put("name", Lists.newArrayList(rule.getId().toHexString()));
            Map<String, List<TextEncryptionRuleDto>> result = service.toRule(fieldRuleIds, new HashSet<>());
            Assertions.assertTrue(result.isEmpty());
        }
    }

    @Nested
    class mapToDtoTest {
        @Test
        void testNormal() {
            when(service.mapToDto(any(TextEncryptionRuleEntity.class))).thenCallRealMethod();
            Assertions.assertNotNull(service.mapToDto(new TextEncryptionRuleEntity()));
        }
    }

    String genericChars(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = count; i > 0; i--) {
            sb.append("a");
        }
        return sb.toString();
    }
}
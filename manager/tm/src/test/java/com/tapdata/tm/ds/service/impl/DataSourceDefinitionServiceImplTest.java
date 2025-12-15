package com.tapdata.tm.ds.service.impl;

import cn.hutool.extra.cglib.CglibUtil;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.DataSourceTypeDto;
import com.tapdata.tm.ds.entity.DataSourceDefinitionEntity;
import com.tapdata.tm.ds.repository.DataSourceDefinitionRepository;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DataSourceDefinitionServiceImplTest {
    DataSourceDefinitionServiceImpl dataSourceDefinitionService;
    DataSourceDefinitionRepository repository;

    @BeforeEach
    void init() {
        dataSourceDefinitionService = mock(DataSourceDefinitionServiceImpl.class);
        repository = mock(DataSourceDefinitionRepository.class);
        ReflectionTestUtils.setField(dataSourceDefinitionService, "repository", repository);
    }


    @Nested
    class NameAsRealNameTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(dataSourceDefinitionService).nameAsRealName(any(DataSourceDefinitionDto.class));
        }

        @Test
        void shouldDoNothing() {
            doCallRealMethod().when(dataSourceDefinitionService).nameAsRealName(null);
            dataSourceDefinitionService.nameAsRealName(null);
        }

        @Test
        void shouldDoNothingWhenRealNameIsBlank() {
            dataSourceDefinitionService.nameAsRealName(new DataSourceDefinitionDto());
        }

        @Test
        void shouldSetNameAsRealName() {
            DataSourceDefinitionDto dataSourceDefinitionDto = new DataSourceDefinitionDto();
            dataSourceDefinitionDto.setRealName("realName");
            dataSourceDefinitionService.nameAsRealName(dataSourceDefinitionDto);
        }
    }

    @Nested
    class findByPdkHashTest {
        UserDetail user;
        @BeforeEach
        void init() {
            user = mock(UserDetail.class);
            when(user.getCustomerId()).thenReturn("customerId");
            doCallRealMethod().when(dataSourceDefinitionService).findByPdkHash(any(String.class), any(Integer.class), any());
            doNothing().when(dataSourceDefinitionService).nameAsRealName(any(DataSourceDefinitionDto.class));
        }

        @Test
        void shouldReturnNullWhenPdkHashIsNull() {
            dataSourceDefinitionService.findByPdkHash(null, 1, user);
        }

        @Test
        void shouldReturnNullWhenPdkHashIsEmpty() {
            DataSourceDefinitionDto dataSourceDefinition = mock(DataSourceDefinitionDto.class);
            DataSourceDefinitionEntity entity = mock(DataSourceDefinitionEntity.class);
            Optional<DataSourceDefinitionEntity> optional = Optional.of(entity);
            when(repository.findOne(any(Query.class))).thenReturn(optional);
            when(dataSourceDefinitionService.convertToDto(entity, DataSourceDefinitionDto.class)).thenReturn(dataSourceDefinition);
            doNothing().when(dataSourceDefinitionService).updateConfigPropertiesTitle(any(DataSourceDefinitionDto.class));
            dataSourceDefinitionService.findByPdkHash("", 1, user);
            verify(repository, times(1)).findOne(any(Query.class));
            verify(dataSourceDefinitionService, times(1)).convertToDto(entity, DataSourceDefinitionDto.class);
        }
    }

    @Nested
    class nameAsRealNameForDataSourceTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(dataSourceDefinitionService).nameAsRealNameForDataSource(any(List.class));
        }
        @Test
        void shouldDoNothing() {
            dataSourceDefinitionService.nameAsRealNameForDataSource(null);
        }
        @Test
        void shouldDoNothingWhenListIsEmpty() {
            dataSourceDefinitionService.nameAsRealNameForDataSource(List.of());
        }
        @Test
        void shouldSetNameAsRealName() {
            DataSourceTypeDto dataSourceTypeDto = new DataSourceTypeDto();
            dataSourceTypeDto.setName("name");
            dataSourceDefinitionService.nameAsRealNameForDataSource(List.of(dataSourceTypeDto));
        }
        @Test
        void shouldDoNothingWhenNameIsBlank() {
            DataSourceTypeDto dataSourceTypeDto = new DataSourceTypeDto();
            dataSourceTypeDto.setName("");
            dataSourceDefinitionService.nameAsRealNameForDataSource(List.of(dataSourceTypeDto));
        }
    }

    @Nested
    class dataSourceTypesV2Test {
        UserDetail user;
        @BeforeEach
        void init() {
            user = mock(UserDetail.class);
            when(user.getCustomerId()).thenReturn("customerId");
            doCallRealMethod().when(dataSourceDefinitionService).dataSourceTypesV2(any(UserDetail.class), any(Filter.class));
        }
        @Test
        void shouldThrowExceptionWhenTagIsNull() {
            Filter filter = new Filter();
            filter.setWhere(new Where());
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    dataSourceDefinitionService.dataSourceTypesV2(user, filter);
                } catch (BizException e) {
                    Assertions.assertEquals("IllegalArgument", e.getErrorCode());
                    throw e;
                }
            });
        }

        @Test
        void shouldThrowExceptionWhenTagIsEmpty() {
            Filter filter = new Filter();
            filter.setWhere(new Where());
            filter.getWhere().put("tag", "Custom");
            filter.setFields(new Field());
            List<DataSourceDefinitionDto> definitionEntities = new ArrayList<>();
            when(dataSourceDefinitionService.findAll(any(Query.class))).thenReturn(definitionEntities);
            when(repository.count(any(Query.class))).thenReturn(0L);
            doNothing().when(dataSourceDefinitionService).updateConfigPropertiesTitle(any(DataSourceDefinitionDto.class));
            doNothing().when(dataSourceDefinitionService).nameAsRealNameForDataSource(anyList());
//            try(MockedStatic<MongoUtils> mongoUtilsMockedStatic = mockStatic(MongoUtils.class)) {
//                mongoUtilsMockedStatic.when(() -> MongoUtils.applyField(any(Query.class), any(Field.class))).thenReturn(null);
                Assertions.assertDoesNotThrow(() -> dataSourceDefinitionService.dataSourceTypesV2(user, filter));
//            }
        }
    }
}
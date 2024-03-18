package com.tapdata.tm.ds.service.impl;


import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import com.tapdata.tm.permissions.service.DataPermissionService;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
@ExtendWith(MockitoExtension.class)
class DataSourceServiceTest {
    private DataSourceService dataSourceServiceUnderTest;
    @Mock
    private DataSourceDefinitionServiceImpl dataSourceDefinitionService;
    @Mock
    private DataSourceRepository dataSourceRepository;

    @BeforeEach
    void setUp() {
        new DataPermissionHelper(mock(IDataPermissionHelper.class)); //when repository.find call methods in DataPermissionHelper class this line is need
        dataSourceServiceUnderTest = new DataSourceServiceImpl(dataSourceRepository);
        ReflectionTestUtils.setField(dataSourceServiceUnderTest,"dataSourceDefinitionService",dataSourceDefinitionService);
    }

    @Test
    void testUpdateConnectionOptions() {
        final ObjectId id = new ObjectId(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(), 0);
        final ConnectionOptions options = new ConnectionOptions();
        options.setDbVersion("dbVersion");
        final UserDetail user = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        Criteria criteria = Criteria.where("_id").is(id);
        Query query = new Query(criteria);
        query.fields().include("_id", "database_type");
        DataSourceEntity dataSourceEntity = new DataSourceEntity();
        dataSourceEntity.setDb_version("test");
        dataSourceEntity.setDatabase_type("mongo");
        DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
        definitionDto.setCapabilities(Arrays.asList(new Capability("id")));
        Update expect = new Update();
        expect.set("db_version","dbVersion");
        expect.set("capabilities",definitionDto.getCapabilities());
        try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)){
            serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
            when(dataSourceRepository.findOne(query,user)).thenReturn(Optional.of(dataSourceEntity));
            when(dataSourceDefinitionService.getByDataSourceType(dataSourceEntity.getDatabase_type(),user)).thenReturn(definitionDto);
            when(dataSourceRepository.updateFirstNotChangeLast(any(),any(),any())).thenAnswer(invocationOnMock -> {
                Update result = invocationOnMock.getArgument(1, Update.class);
                Assertions.assertEquals(expect,result);
                return null;
            });
            dataSourceServiceUnderTest.updateConnectionOptions(id, options, user);
        }
    }

    @Test
    void testUpdateConnectionOptions_dbVersoinNull() {
        final ObjectId id = new ObjectId(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(), 0);
        final ConnectionOptions options = new ConnectionOptions();
        final UserDetail user = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        Criteria criteria = Criteria.where("_id").is(id);
        Query query = new Query(criteria);
        query.fields().include("_id", "database_type");
        DataSourceEntity dataSourceEntity = new DataSourceEntity();
        dataSourceEntity.setDb_version("test");
        dataSourceEntity.setDatabase_type("mongo");
        DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
        definitionDto.setCapabilities(Arrays.asList(new Capability("id")));
        Update expect = new Update();
        expect.set("capabilities",definitionDto.getCapabilities());
        try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)){
            serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
            when(dataSourceRepository.findOne(query,user)).thenReturn(Optional.of(dataSourceEntity));
            when(dataSourceDefinitionService.getByDataSourceType(dataSourceEntity.getDatabase_type(),user)).thenReturn(definitionDto);
            when(dataSourceRepository.updateFirstNotChangeLast(any(),any(),any())).thenAnswer(invocationOnMock -> {
                Update result = invocationOnMock.getArgument(1, Update.class);
                Assertions.assertNull(result.getUpdateObject().get("db_version"));
                return null;
            });
            dataSourceServiceUnderTest.updateConnectionOptions(id, options, user);
        }
    }
}

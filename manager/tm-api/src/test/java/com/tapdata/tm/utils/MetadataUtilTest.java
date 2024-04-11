package com.tapdata.tm.utils;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataUtilTest {
    @Nested
    class ModelNext{
        MetadataUtil metadataUtil = new MetadataUtil();
        MetadataInstancesService metadataInstancesService;
        @BeforeEach
        void before(){
            metadataInstancesService = mock(MetadataInstancesService.class);
            ReflectionTestUtils.setField(metadataUtil,"metadataInstancesService",metadataInstancesService);
        }

        @Test
        void test_updateOldModel(){
            try (MockedStatic<MetaDataBuilderUtils> mockedStatic = Mockito.mockStatic(MetaDataBuilderUtils.class)){
                mockedStatic.when(()->MetaDataBuilderUtils.generateQualifiedName(any(),any(DataSourceConnectionDto.class),any())).thenReturn("test-qualified");
                List<MetadataInstancesDto> input = new ArrayList<>();
                MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
                metadataInstancesDto.setMetaType("test");
                metadataInstancesDto.setOriginalName("test");
                metadataInstancesDto.setLastUpdate(1L);
                input.add(metadataInstancesDto);
                List<MetadataInstancesDto> oldMetadataInstances = new ArrayList<>();
                MetadataInstancesDto old = new MetadataInstancesDto();
                old.setQualifiedName("test-qualified");
                old.setMetaType("test");
                old.setOriginalName("test");
                old.setLastUpdate(0L);
                oldMetadataInstances.add(old);
                when(metadataInstancesService.findAllDto(any(),any())).thenReturn(oldMetadataInstances);
                List<MetadataInstancesDto> result = metadataUtil.modelNext(input,mock(DataSourceConnectionDto.class),"test",mock(UserDetail.class));
                Assertions.assertEquals(1,result.size());
            }

        }

        @Test
        void test_notUpdateOldModel(){
            try (MockedStatic<MetaDataBuilderUtils> mockedStatic = Mockito.mockStatic(MetaDataBuilderUtils.class)){
                mockedStatic.when(()->MetaDataBuilderUtils.generateQualifiedName(any(),any(DataSourceConnectionDto.class),any())).thenReturn("test-qualified");
                List<MetadataInstancesDto> input = new ArrayList<>();
                MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
                metadataInstancesDto.setMetaType("test");
                metadataInstancesDto.setOriginalName("test");
                metadataInstancesDto.setLastUpdate(0L);
                input.add(metadataInstancesDto);
                List<MetadataInstancesDto> oldMetadataInstances = new ArrayList<>();
                MetadataInstancesDto old = new MetadataInstancesDto();
                old.setQualifiedName("test-qualified");
                old.setMetaType("test");
                old.setOriginalName("test");
                old.setLastUpdate(0L);
                oldMetadataInstances.add(old);
                when(metadataInstancesService.findAllDto(any(),any())).thenReturn(oldMetadataInstances);
                List<MetadataInstancesDto> result = metadataUtil.modelNext(input,mock(DataSourceConnectionDto.class),"test",mock(UserDetail.class));
                Assertions.assertEquals(1,result.size());
            }
        }

        @Test
        void test_oldModelIsNull(){
            try (MockedStatic<MetaDataBuilderUtils> mockedStatic = Mockito.mockStatic(MetaDataBuilderUtils.class)){
                mockedStatic.when(()->MetaDataBuilderUtils.generateQualifiedName(any(),any(DataSourceConnectionDto.class),any())).thenReturn("test-qualified");
                List<MetadataInstancesDto> input = new ArrayList<>();
                MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
                metadataInstancesDto.setMetaType("test");
                metadataInstancesDto.setOriginalName("test");
                metadataInstancesDto.setLastUpdate(0L);
                input.add(metadataInstancesDto);
                when(metadataInstancesService.findAllDto(any(),any())).thenReturn(new ArrayList<>());
                List<MetadataInstancesDto> result = metadataUtil.modelNext(input,mock(DataSourceConnectionDto.class),"test",mock(UserDetail.class));
                Assertions.assertEquals(1,result.size());
            }
        }

        @Test
        void test_oldModelLastUpdateIsNull(){
            try (MockedStatic<MetaDataBuilderUtils> mockedStatic = Mockito.mockStatic(MetaDataBuilderUtils.class)){
                mockedStatic.when(()->MetaDataBuilderUtils.generateQualifiedName(any(),any(DataSourceConnectionDto.class),any())).thenReturn("test-qualified");
                List<MetadataInstancesDto> input = new ArrayList<>();
                MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
                metadataInstancesDto.setMetaType("test");
                metadataInstancesDto.setOriginalName("test");
                metadataInstancesDto.setLastUpdate(0L);
                input.add(metadataInstancesDto);
                List<MetadataInstancesDto> oldMetadataInstances = new ArrayList<>();
                MetadataInstancesDto old = new MetadataInstancesDto();
                old.setQualifiedName("test-qualified");
                old.setMetaType("test");
                old.setOriginalName("test");
                old.setLastUpdate(null);
                oldMetadataInstances.add(old);
                when(metadataInstancesService.findAllDto(any(),any())).thenReturn(oldMetadataInstances);
                List<MetadataInstancesDto> result = metadataUtil.modelNext(input,mock(DataSourceConnectionDto.class),"test",mock(UserDetail.class));
                Assertions.assertEquals(1,result.size());
            }
        }

        @Test
        void test_newModelLastUpdateIsNull(){
            try (MockedStatic<MetaDataBuilderUtils> mockedStatic = Mockito.mockStatic(MetaDataBuilderUtils.class)){
                mockedStatic.when(()->MetaDataBuilderUtils.generateQualifiedName(any(),any(DataSourceConnectionDto.class),any())).thenReturn("test-qualified");
                List<MetadataInstancesDto> input = new ArrayList<>();
                MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
                metadataInstancesDto.setMetaType("test");
                metadataInstancesDto.setOriginalName("test");
                metadataInstancesDto.setLastUpdate(null);
                input.add(metadataInstancesDto);
                List<MetadataInstancesDto> oldMetadataInstances = new ArrayList<>();
                MetadataInstancesDto old = new MetadataInstancesDto();
                old.setQualifiedName("test-qualified");
                old.setMetaType("test");
                old.setOriginalName("test");
                old.setLastUpdate(1L);
                oldMetadataInstances.add(old);
                when(metadataInstancesService.findAllDto(any(),any())).thenReturn(oldMetadataInstances);
                List<MetadataInstancesDto> result = metadataUtil.modelNext(input,mock(DataSourceConnectionDto.class),"test",mock(UserDetail.class));
                Assertions.assertEquals(1,result.size());
            }
        }

    }
}

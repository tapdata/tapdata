package com.tapdata.tm.modules.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.dto.ModulesPermissionsDto;
import com.tapdata.tm.modules.dto.ModulesTagsDto;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.repository.ModulesRepository;
import org.junit.jupiter.api.*;
import org.mockito.internal.verification.Times;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class ModulesServiceTest {
    ModulesService modulesService;
    ModulesRepository modulesRepository;
    @BeforeEach
    void init(){
        modulesRepository = mock(ModulesRepository.class);
        modulesService = new ModulesService(modulesRepository);
    }
    @Nested
    class UpdatePermissionsTest{
        @Test
        void test_main(){
            ModulesPermissionsDto modulesPermissionsDto = new ModulesPermissionsDto();
            modulesPermissionsDto.setModuleId("test");
            modulesPermissionsDto.setAcl(Arrays.asList("admin"));
            modulesService.updatePermissions(modulesPermissionsDto,mock(UserDetail.class));
            verify(modulesRepository,times(1)).updateFirst(any(),any(),any());
        }

        @Test
        void test_aclIsNull(){
            ModulesPermissionsDto modulesPermissionsDto = new ModulesPermissionsDto();
            modulesPermissionsDto.setModuleId("test");
            assertThrows(BizException.class,()->modulesService.updatePermissions(modulesPermissionsDto,mock(UserDetail.class)));
        }

    }

    @Nested
    class UpdateTagsTest{
        @Test
        void test_main(){
            ModulesTagsDto modulesTagsDto = new ModulesTagsDto();
            modulesTagsDto.setModuleId("test");
            modulesTagsDto.setListtags(Arrays.asList(new Tag("id","app")));
            modulesService.updateTags(modulesTagsDto,mock(UserDetail.class));
            verify(modulesRepository,times(1)).updateFirst(any(),any(),any());
        }

        @Test
        void test_tagsIsNull(){
            ModulesTagsDto modulesTagsDto = new ModulesTagsDto();
            modulesTagsDto.setModuleId("test");
            assertThrows(BizException.class,()->modulesService.updateTags(modulesTagsDto,mock(UserDetail.class)));
        }

    }
    @Nested
    class BeforeSaveTest{
        @Test
        void test_main(){
            ModulesDto modules = new ModulesDto();
            List<Path> paths = new ArrayList<>();
            Path path = new Path();
            List<Field> fields = new ArrayList<>();
            fields.add(new Field());
            fields.add(null);
            fields.add(null);
            path.setFields(fields);
            paths.add(path);
            modules.setPaths(paths);
            modulesService.beforeSave(modules,mock(UserDetail.class));
            Assertions.assertEquals(1,modules.getPaths().get(0).getFields().size());
        }
        @Test
        void test_paths_isNull(){
            ModulesDto modules = new ModulesDto();
            List<Path> paths = new ArrayList<>();
            modules.setPaths(paths);
            modulesService.beforeSave(modules,mock(UserDetail.class));
            Assertions.assertEquals(0,modules.getPaths().size());
        }
        @Test
        void test_paths_fields_isNull(){
            ModulesDto modules = new ModulesDto();
            List<Path> paths = new ArrayList<>();
            Path path = new Path();
            List<Field> fields = new ArrayList<>();
            path.setFields(fields);
            paths.add(path);
            modules.setPaths(paths);
            modulesService.beforeSave(modules,mock(UserDetail.class));
            Assertions.assertEquals(0,modules.getPaths().get(0).getFields().size());
        }
    }
    @Nested
    class saveTest{
        private ModulesDto modulesDto;
        private UserDetail userDetail;
        @BeforeEach
        void beforeEach(){
            modulesService = spy(modulesService);
            modulesDto = mock(ModulesDto.class);
            userDetail = mock(UserDetail.class);
        }
        @Test
        @DisplayName("test save method when name existed")
        void test1(){
            String name = "test";
            when(modulesDto.getName()).thenReturn(name);
            List<ModulesDto> modules = new ArrayList<>();
            modules.add(mock(ModulesDto.class));
            modules.add(mock(ModulesDto.class));
            doReturn(modules).when(modulesService).findByName(name);
            assertThrows(BizException.class, ()->modulesService.save(modulesDto, userDetail));
        }
        @Test
        @DisplayName("test save method normal")
        void test2(){
            String name = "test";
            when(modulesDto.getName()).thenReturn(name);
            List<ModulesDto> modules = new ArrayList<>();
            doReturn(modules).when(modulesService).findByName(name);
            doCallRealMethod().when(modulesService).save(modulesDto, userDetail);
            when(modulesRepository.save(any(),any())).thenReturn(mock(ModulesEntity.class));
            modulesService.save(modulesDto, userDetail);
            verify(modulesRepository).save(any(),any());
        }
    }
    @Nested
    class batchUpdateModuleByListTest{
        private List<ModulesDto> modulesDtos;
        private UserDetail userDetail;
        @Test
        void testBatchUpdateModuleByListNormal(){
            modulesService = spy(modulesService);
            modulesDtos = new ArrayList<>();
            ModulesDto modulesDto = mock(ModulesDto.class);
            modulesDtos.add(modulesDto);
            userDetail = mock(UserDetail.class);
            doReturn(modulesDto).when(modulesService).updateModuleById(modulesDto, userDetail);
            modulesService.batchUpdateModuleByList(modulesDtos, userDetail);
            verify(modulesService,new Times(1)).updateModuleById(modulesDto, userDetail);
        }
    }
}
